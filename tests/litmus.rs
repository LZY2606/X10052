//! Litmus suite for the debt-helping / guard-fallback / CAS linearization-point audit.
//!
//! Every test in here is paired with a design assumption it is able to falsify (see
//! `ANALYSIS.md`, section "Litmus 对照表"). Interleavings are driven by barriers only —
//! no sleeps, no wall-clock assumptions — so the assertions are deterministic for every
//! run, even though the exact interleaving of atomic operations is chosen by the
//! scheduler.
//!
//! The tests can be located and run individually, eg:
//!
//! ```text
//! cargo test --features internal-test-strategies --test litmus guards_beyond_fast_slots
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use arc_swap::strategy::{CaS, Strategy};
use arc_swap::{ArcSwap, ArcSwapAny, DefaultStrategy, Guard};

#[cfg(feature = "internal-test-strategies")]
use std::sync::RwLock;
#[cfg(feature = "internal-test-strategies")]
#[allow(deprecated)] // Internal testing strategy, used on purpose here.
use arc_swap::strategy::test_strategies::FillFastSlots;

/// Number of fast debt slots each thread owns (`DEBT_SLOT_CNT` in `src/debt/fast.rs`).
///
/// The audit pins the observable consequences of this constant: guards beyond this many
/// per thread must come from the helping fallback and therefore own a full ref count.
const FAST_SLOTS: usize = 8;

#[cfg(not(miri))]
const CHURN_ITERS: usize = 400;
#[cfg(miri)]
const CHURN_ITERS: usize = 30;

/// Counts constructions and destructions of every `Tracked` generation.
///
/// A double drop or a leak shows up as a drop count different from 1, with the
/// generation id attached for diagnosis.
struct Registry {
    created: AtomicUsize,
    drops: Vec<AtomicUsize>,
}

impl Registry {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            created: AtomicUsize::new(0),
            drops: (0..capacity).map(|_| AtomicUsize::new(0)).collect(),
        })
    }

    fn track(self: &Arc<Self>, value: usize) -> Arc<Tracked> {
        let id = self.created.fetch_add(1, Ordering::SeqCst);
        assert!(
            id < self.drops.len(),
            "registry capacity {} exhausted by generation {}",
            self.drops.len(),
            id,
        );
        Arc::new(Tracked {
            id,
            value,
            registry: Arc::clone(self),
        })
    }

    fn drops_of(&self, id: usize) -> usize {
        self.drops[id].load(Ordering::SeqCst)
    }

    fn created(&self) -> usize {
        self.created.load(Ordering::SeqCst)
    }

    /// Every generated value must be dropped exactly once: no double drop, no leak.
    fn assert_all_dropped_once(&self, ctx: &str) {
        for id in 0..self.created() {
            let drops = self.drops_of(id);
            assert_eq!(
                1, drops,
                "{}: generation {} dropped {} times (expected exactly 1)",
                ctx, id, drops,
            );
        }
    }
}

struct Tracked {
    id: usize,
    value: usize,
    registry: Arc<Registry>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.registry.drops[self.id].fetch_add(1, Ordering::SeqCst);
    }
}

/// A thread may hold more live guards than there are fast debt slots.
///
/// Falsifies the assumption "8 fast slots per thread are always enough" — guards beyond
/// the 8th must come from the helping fallback and own a full ref count, the writer must
/// pay exactly the 8 fast debts, and the old value must be dropped exactly once.
#[test]
fn guards_beyond_fast_slots_use_fallback() {
    let registry = Registry::new(2);
    let a = ArcSwap::<Tracked>::from(registry.track(0));
    assert_eq!(1, Arc::strong_count(&a.load()), "storage owns the only ref");

    let guards: Vec<_> = (0..FAST_SLOTS * 2).map(|_| a.load()).collect();
    assert!(
        guards.iter().all(|g| g.value == 0),
        "all guards must observe generation 0"
    );
    // FAST_SLOTS guards owe a debt (no ref count held), the rest own a full Arc each.
    let expected = 1 + FAST_SLOTS;
    assert_eq!(
        expected,
        Arc::strong_count(&guards[0]),
        "guards past the fast slots must own a full ref count each",
    );

    a.store(registry.track(1));
    // The writer paid exactly the FAST_SLOTS debts; the storage ref is gone.
    assert_eq!(
        FAST_SLOTS * 2,
        Arc::strong_count(&guards[0]),
        "writer must pay each fast-slot debt exactly once",
    );

    drop(guards);
    assert_eq!(
        1,
        registry.drops_of(0),
        "generation 0 must be dropped exactly once after the last guard",
    );
    assert_eq!(1, Arc::strong_count(&a.load()), "new value only in storage");

    drop(a);
    registry.assert_all_dropped_once("guards_beyond_fast_slots_use_fallback");
}

/// `Guard::into_inner` converts both debt-protected and fallback guards into owned ref
/// counts, releasing the debt slot for further loads.
///
/// Falsifies the assumption "into_inner can forget to pay the debt back" — that would
/// either leak the slot (later fallback loads misbehave) or double-pay it (double drop).
#[test]
fn guard_into_inner_releases_debt() {
    let registry = Registry::new(2);
    let a = ArcSwap::<Tracked>::from(registry.track(0));

    let guards: Vec<_> = (0..FAST_SLOTS + 2).map(|_| a.load()).collect();
    let arcs: Vec<_> = guards.into_iter().map(Guard::into_inner).collect();
    let owned = FAST_SLOTS + 2;
    assert_eq!(
        owned + 1,
        Arc::strong_count(&arcs[0]),
        "every into_inner must yield an owned ref count on top of the storage ref",
    );

    a.store(registry.track(1));
    assert_eq!(
        owned,
        Arc::strong_count(&arcs[0]),
        "no debts may remain for the writer to pay after into_inner",
    );

    drop(arcs);
    assert_eq!(1, registry.drops_of(0), "generation 0 dropped exactly once");

    drop(a);
    registry.assert_all_dropped_once("guard_into_inner_releases_debt");
}

/// Reader and writer in lock-step phases: the writer replaces the value (and pays debts
/// in `wait_for_readers`) while the reader keeps loading. The reader must only ever
/// observe values tracked by this registry (no torn/dangling pointers) and every
/// generation must be dropped exactly once at the end.
///
/// Falsifies the assumption "debt helping can hand over an unprotected or
/// double-protected pointer" — either shows up as a leak, a double drop, or (under
/// miri) as use-after-free.
fn churn_exactly_once<S>(label: &str, iters: usize)
where
    S: Default + Send + Sync + Strategy<Arc<Tracked>>,
{
    let registry = Registry::new(iters + 1);
    let a = ArcSwapAny::<Arc<Tracked>, S>::from(registry.track(0));
    let barrier = Barrier::new(2);
    thread::scope(|scope| {
        scope.spawn(|| {
            for _ in 0..iters {
                barrier.wait();
                let guard = a.load();
                assert!(
                    guard.id < registry.created(),
                    "{}: reader observed untracked generation {}",
                    label,
                    guard.id,
                );
                assert!(
                    Arc::ptr_eq(&guard.registry, &registry),
                    "{}: reader observed a foreign value",
                    label,
                );
                barrier.wait();
            }
        });
        scope.spawn(|| {
            for i in 0..iters {
                barrier.wait();
                a.store(registry.track(i + 1));
                barrier.wait();
            }
        });
    });
    drop(a);
    registry.assert_all_dropped_once(label);
}

#[test]
fn churn_default_strategy() {
    churn_exactly_once::<DefaultStrategy>("churn_default_strategy", CHURN_ITERS);
}

/// Same as above, but with the fast slots disabled, so *every* load goes through the
/// helping fallback and every store may actively help a reader in flight.
#[cfg(feature = "internal-test-strategies")]
#[test]
#[allow(deprecated)] // Internal testing strategy, used on purpose.
fn churn_forced_fallback() {
    churn_exactly_once::<FillFastSlots>("churn_forced_fallback", CHURN_ITERS);
}

/// Cross-check: the trivially-correct RwLock strategy must satisfy the same invariants
/// under the identical interleaving harness.
#[cfg(feature = "internal-test-strategies")]
#[test]
fn churn_rw_lock_cross_check() {
    churn_exactly_once::<RwLock<()>>("churn_rw_lock_cross_check", CHURN_ITERS);
}

/// Two CAS operations with the same `current`: exactly one may win.
///
/// Falsifies the assumption "compare_and_swap can linearize twice for one store" — the
/// atomic compare_exchange is the single linearization point, so one thread must
/// observe the other's value and fail. The final value must be the winner's and every
/// generation (including the loser's never-stored candidate) dropped exactly once.
fn cas_exactly_one_winner<S>(label: &str)
where
    S: Default + Send + Sync + CaS<Arc<Tracked>>,
{
    let registry = Registry::new(3);
    let a = ArcSwapAny::<Arc<Tracked>, S>::from(registry.track(0));
    let cur = a.load_full();
    let barrier = Barrier::new(3);
    thread::scope(|scope| {
        let new1 = registry.track(1);
        let new2 = registry.track(2);
        let a = &a;
        let cur = &cur;
        let barrier = &barrier;
        let cas1 = scope.spawn(move || {
            barrier.wait();
            Guard::into_inner(a.compare_and_swap(cur, new1))
        });
        let cas2 = scope.spawn(move || {
            barrier.wait();
            Guard::into_inner(a.compare_and_swap(cur, new2))
        });
        barrier.wait();
        let ret1 = cas1.join().expect("first CAS thread panicked");
        let ret2 = cas2.join().expect("second CAS thread panicked");

        let won1 = Arc::ptr_eq(&ret1, &cur);
        let won2 = Arc::ptr_eq(&ret2, &cur);
        assert_ne!(
            won1, won2,
            "{}: exactly one CAS must win (won1={}, won2={})",
            label, won1, won2,
        );
        let winning_value = if won1 { 1 } else { 2 };
        assert_eq!(
            winning_value,
            a.load().value,
            "{}: final value must be the winner's",
            label,
        );
        let loser = if won1 { &ret2 } else { &ret1 };
        assert_eq!(
            winning_value, loser.value,
            "{}: the losing CAS must observe the winner's value",
            label,
        );
    });
    drop(cur);
    drop(a);
    registry.assert_all_dropped_once(label);
}

#[test]
fn cas_exactly_one_winner_default() {
    cas_exactly_one_winner::<DefaultStrategy>("cas_exactly_one_winner_default");
}

#[cfg(feature = "internal-test-strategies")]
#[test]
#[allow(deprecated)] // Internal testing strategy, used on purpose.
fn cas_exactly_one_winner_forced_fallback() {
    cas_exactly_one_winner::<FillFastSlots>("cas_exactly_one_winner_forced_fallback");
}

#[cfg(feature = "internal-test-strategies")]
#[test]
fn cas_exactly_one_winner_rw_lock() {
    cas_exactly_one_winner::<RwLock<()>>("cas_exactly_one_winner_rw_lock");
}

/// An unconditional `store` racing a `compare_and_swap`.
///
/// Falsifies the assumption "a failed CAS can observe something else than the value
/// that made it fail". The store always determines the final state; the CAS either
/// linearizes before it (and wins) or after it (and must then observe the stored
/// value). All three generations must be dropped exactly once.
fn store_vs_cas_race<S>(label: &str)
where
    S: Default + Send + Sync + CaS<Arc<Tracked>>,
{
    let registry = Registry::new(3);
    let a = ArcSwapAny::<Arc<Tracked>, S>::from(registry.track(0));
    let cur = a.load_full();
    let barrier = Barrier::new(2);
    thread::scope(|scope| {
        let new = registry.track(1);
        let a = &a;
        let cur = &cur;
        let barrier = &barrier;
        let registry = &registry;
        let cas = scope.spawn(move || {
            barrier.wait();
            Guard::into_inner(a.compare_and_swap(cur, new))
        });
        let store = scope.spawn(move || {
            barrier.wait();
            a.store(registry.track(2));
        });
        let guard = cas.join().expect("CAS thread panicked");
        store.join().expect("store thread panicked");

        assert_eq!(
            2,
            a.load().value,
            "{}: the unconditional store must decide the final state",
            label,
        );
        if Arc::ptr_eq(&guard, &cur) {
            // The CAS linearized before the store: legal.
        } else {
            assert_eq!(
                2, guard.value,
                "{}: a failed CAS must observe the value that replaced its `current`",
                label,
            );
        }
    });
    drop(cur);
    drop(a);
    registry.assert_all_dropped_once(label);
}

#[test]
fn store_vs_cas_race_default() {
    store_vs_cas_race::<DefaultStrategy>("store_vs_cas_race_default");
}

#[cfg(feature = "internal-test-strategies")]
#[test]
#[allow(deprecated)] // Internal testing strategy, used on purpose.
fn store_vs_cas_race_forced_fallback() {
    store_vs_cas_race::<FillFastSlots>("store_vs_cas_race_forced_fallback");
}

#[cfg(feature = "internal-test-strategies")]
#[test]
fn store_vs_cas_race_rw_lock() {
    store_vs_cas_race::<RwLock<()>>("store_vs_cas_race_rw_lock");
}

/// A deterministic, single-threaded script of operations whose observable results
/// (loaded values, identities of returned pointers, CAS success flags) are recorded
/// step by step. The trace produced by the hybrid strategies must equal the trace of
/// the RwLock strategy, which is simple enough to be trusted by inspection.
///
/// Falsifies the assumption "the debt machinery changes observable semantics" — any
/// leaked debt, double pay, or wrong linearization shows up as a diverging step.
#[cfg(feature = "internal-test-strategies")]
fn step_trace<S>() -> Vec<String>
where
    S: Default + CaS<Arc<usize>>,
{
    let pool: Vec<Arc<usize>> = (0..4).map(Arc::new).collect();
    let a = ArcSwapAny::<Arc<usize>, S>::from(Arc::clone(&pool[0]));
    let mut trace = Vec::new();
    let index_of = |ptr: &Arc<usize>| -> String {
        pool.iter()
            .position(|candidate| Arc::ptr_eq(candidate, ptr))
            .map(|i| i.to_string())
            .unwrap_or_else(|| "foreign".to_owned())
    };

    a.store(Arc::clone(&pool[1]));
    trace.push(format!("load={}", **a.load()));

    let prev = a.swap(Arc::clone(&pool[2]));
    trace.push(format!("swap-returned={}", index_of(&prev)));

    let won = a.compare_and_swap(&pool[2], Arc::clone(&pool[3]));
    trace.push(format!("cas-won={}", Arc::ptr_eq(&won, &pool[2])));

    let lost = a.compare_and_swap(&pool[2], Arc::clone(&pool[0]));
    trace.push(format!(
        "cas-lost={}:returned={}",
        Arc::ptr_eq(&lost, &pool[2]),
        index_of(&lost),
    ));

    trace.push(format!("load_full={}", *a.load_full()));
    trace
}

#[cfg(feature = "internal-test-strategies")]
#[test]
#[allow(deprecated)] // Internal testing strategy, used on purpose.
fn step_trace_matches_rw_lock() {
    let hybrid = step_trace::<DefaultStrategy>();
    let fallback = step_trace::<FillFastSlots>();
    let rw_lock = step_trace::<RwLock<()>>();
    assert_eq!(
        rw_lock, hybrid,
        "default strategy diverges from the RwLock strategy",
    );
    assert_eq!(
        rw_lock, fallback,
        "forced-fallback strategy diverges from the RwLock strategy",
    );
}
