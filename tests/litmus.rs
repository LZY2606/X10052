//! Deterministic litmus suite for the audit:
//! "debt helping, guard fallback and CAS linearization points".
//!
//! Unlike `tests/stress.rs` (randomized torture) and `tests/random.rs` (property sequences),
//! every test in here drives an *explicit* interleaving through the test-only hooks exposed by
//! `arc_swap::strategy::test_hooks`. There are no sleeps and no timing assumptions: a parked
//! thread is released by the orchestrator after the other thread has been forced through the
//! audited window.
//!
//! The file has three parts:
//!
//! 1. A [`controller`] that registers a hook and parks/releases threads at armed hook points.
//! 2. Real-code litmus tests that run the actual `DefaultStrategy` algorithm and cross-check the
//!    outcomes against the reference `RwLock<()>` strategy at sequential boundaries.
//! 3. A small executable model of the debt protocol ([`model`]) with "broken variant" switches.
//!    The same scripted histories are replayed against the correct and broken protocols, so the
//!    tests actually *refute* the design hypotheses instead of only exercising happy paths.

use std::ptr;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::thread::ThreadId;

use arc_swap::strategy::test_hooks::{self, HookPoint};
use arc_swap::strategy::{CaS, DefaultStrategy, Strategy};
use arc_swap::{ArcSwap, ArcSwapAny, Guard, RefCnt};
use crossbeam_utils::thread;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicPtr, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, RwLock};

// ------------------------------------------------------------------------------------------------
// Global serialization + drop observation
// ------------------------------------------------------------------------------------------------

/// The hooked algorithm is global (debts live in a process-wide thread-node list), therefore the
/// orchestrated tests run one at a time.
static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A value that records its own drop. Every drop is appended to the shared log as a pair
/// `(tag, unique allocation id)`, so tests can assert every allocation drops exactly once and in
/// the right phase of the schedule.
#[derive(Debug)]
struct Canary {
    tag: &'static str,
    id: u64,
    drops: Arc<Mutex<Vec<(&'static str, u64)>>>,
}

impl PartialEq for Canary {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        self.drops.lock().unwrap().push((self.tag, self.id));
    }
}

fn canary(tag: &'static str, id: u64, drops: &Arc<Mutex<Vec<(&'static str, u64)>>>) -> Arc<Canary> {
    Arc::new(Canary {
        tag,
        id,
        drops: Arc::clone(drops),
    })
}

// ------------------------------------------------------------------------------------------------
// Deterministic interleave controller
// ------------------------------------------------------------------------------------------------

mod controller {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Action {
        Park,
        Pass,
    }

    struct State {
        /// Main thread never parks.
        main: ThreadId,
        /// Armed point + optional storage-address filter.
        armed: Option<(HookPoint, Option<usize>)>,
        action: Action,
        /// Identity of the currently parked thread, if any.
        parked: Option<ThreadId>,
        /// All hook observations in arrival order (point, storage address).
        hits: Vec<(HookPoint, usize)>,
    }

    struct Inner {
        mutex: Mutex<State>,
        cv: Condvar,
    }

    /// The process-wide context read by the trampoline. At most one controller exists at a time
    /// (tests hold [`TEST_LOCK`]).
    static CONTEXT: AtomicPtr<Inner> = AtomicPtr::new(ptr::null_mut());

    /// RAII registration of the global hook. Clearing the hook on drop guarantees no callback
    /// ever outlives the test state.
    pub struct Interleave {
        inner: &'static Inner,
    }

    impl Interleave {
        pub fn new() -> Self {
            let inner: &'static Inner = Box::leak(Box::new(Inner {
                mutex: Mutex::new(State {
                    main: std::thread::current().id(),
                    armed: None,
                    action: Action::Pass,
                    parked: None,
                    hits: Vec::new(),
                }),
                cv: Condvar::new(),
            }));
            CONTEXT.store(inner as *const _ as *mut Inner, AtomicOrdering::Release);
            test_hooks::set_hook(Some(trampoline));
            Interleave { inner }
        }

        fn lock(&self) -> MutexGuard<'_, State> {
            self.inner
                .mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        /// Arms `point`; the first *other* thread that reaches it (on the given storage address,
        /// when provided) parks.
        pub fn arm(&self, point: HookPoint, storage: Option<usize>) {
            let mut s = self.lock();
            assert!(
                s.parked.is_none(),
                "arm({:?}) while another thread is parked",
                point,
            );
            s.armed = Some((point, storage));
            s.action = Action::Park;
        }

        /// Waits until a non-main thread is parked at the armed point.
        pub fn wait_parked(&self) {
            let mut s = self.lock();
            loop {
                if s.parked.is_some() {
                    return;
                }
                let (guard, timeout) = self
                    .inner
                    .cv
                    .wait_timeout(s, std::time::Duration::from_secs(10))
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                s = guard;
                assert!(
                    !timeout.timed_out(),
                    "no thread reached the armed hook within 10s (hits so far: {:?})",
                    s.hits,
                );
            }
        }

        /// Releases the parked thread. Callers afterwards synchronize with a `Barrier` so the
        /// worker is known to have completed its audited operation.
        pub fn release(&self) {
            let mut s = self.lock();
            assert!(s.parked.is_some(), "release() with no parked thread");
            s.parked = None;
            s.armed = None;
            s.action = Action::Pass;
            self.inner.cv.notify_all();
        }

        /// Snapshot of observed hook points.
        pub fn hits(&self) -> Vec<(HookPoint, usize)> {
            self.lock().hits.clone()
        }

        /// Hook point the currently parked thread is held at, if any.
        pub fn parked_on(&self) -> Option<HookPoint> {
            let s = self.lock();
            if s.parked.is_some() {
                s.armed.map(|(p, _)| p)
            } else {
                None
            }
        }

        /// Blocks until a hook for `point` on `storage` has been observed by any thread.
        #[allow(dead_code)] // Utility for future litmus additions.
        pub fn wait_for_hit(&self, point: HookPoint, storage: usize) {
            let mut s = self.lock();
            loop {
                if s.hits.iter().any(|(p, a)| *p == point && *a == storage) {
                    return;
                }
                let (guard, timeout) = self
                    .inner
                    .cv
                    .wait_timeout(s, std::time::Duration::from_secs(10))
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                s = guard;
                assert!(
                    !timeout.timed_out(),
                    "expected hook hit {:?} on {:x} did not happen (hits: {:?})",
                    point,
                    storage,
                    s.hits,
                );
            }
        }

        /// Blocks until the pay-all walk reports both start and at least one node completion.
        pub fn wait_pay_all_done(&self) {
            let mut s = self.lock();
            loop {
                let started = s
                    .hits
                    .iter()
                    .any(|(p, _)| *p == HookPoint::WriterPayAllStart);
                let slots_done = s
                    .hits
                    .iter()
                    .any(|(p, _)| *p == HookPoint::WriterNodeSlotsDone);
                if started && slots_done {
                    return;
                }
                let (guard, timeout) = self
                    .inner
                    .cv
                    .wait_timeout(s, std::time::Duration::from_secs(10))
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                s = guard;
                assert!(!timeout.timed_out(), "pay-all walk did not finish: {:?}", s.hits);
            }
        }
    }

    impl Drop for Interleave {
        fn drop(&mut self) {
            test_hooks::set_hook(None);
            // Full barrier via the lock makes sure a thread still inside the callback observes
            // the cleared hook path before the boxed inner is abandoned.
            let mut s = self.lock();
            if s.parked.is_some() {
                s.parked = None;
                s.armed = None;
                s.action = Action::Pass;
                self.inner.cv.notify_all();
            }
            CONTEXT.store(ptr::null_mut(), AtomicOrdering::Release);
        }
    }

    fn trampoline(point: HookPoint, storage: usize) {
        let inner = CONTEXT.load(AtomicOrdering::Acquire);
        if inner.is_null() {
            return;
        }
        let inner = unsafe { &*inner };
        let me = std::thread::current().id();
        let mut s = inner
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.hits.push((point, storage));
        inner.cv.notify_all();
        if me == s.main {
            return;
        }
        if let Some((armed, addr)) = s.armed {
            let addr_matches = addr.map_or(true, |a| a == storage);
            if armed == point && addr_matches && s.action == Action::Park {
                s.parked = Some(me);
                inner.cv.notify_all();
                while s.parked == Some(me) {
                    s = inner
                        .cv
                        .wait(s)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
        }
    }
}

use controller::Interleave;

fn storage_addr<T: RefCnt>(s: &ArcSwapAny<T, DefaultStrategy>) -> usize {
    s as *const _ as usize
}

fn storage_addr_generic<T: RefCnt, S: Strategy<T>>(s: &ArcSwapAny<T, S>) -> usize {
    s as *const _ as usize
}

// ================================================================================================
// Real-code litmus tests
// ================================================================================================

/// Hypothesis refuted: "more than 8 simultaneously held guards on one thread are impossible /
/// exhaust the protection".
///
/// Reality: the 8 fast slots cover the first 8 guards; every further guard transparently goes
/// through the helping/fallback transaction and owns a full `Arc`. The fallback transaction
/// *releases* the helping slot on completion, so it can be reused arbitrarily many times.
///
/// The same single-threaded, contention-free trace is replayed against `DefaultStrategy`, the
/// `FillFastSlots` test strategy (fast path disabled, every load is a fallback) and the
/// `RwLock<()>` reference strategy; observed values and drop traces must agree.
#[test]
fn litmus_more_than_eight_guards_same_thread() {
    let _g = lock();
    run_guard_overflow::<DefaultStrategy>("default");
    #[allow(deprecated)]
    run_guard_overflow::<arc_swap::strategy::test_strategies::FillFastSlots>("no-fast");
    run_guard_overflow::<RwLock<()>>("rwlock");
}

fn run_guard_overflow<S>(strategy_name: &str)
where
    S: Default + Send + Sync + Strategy<Arc<Canary>> + CaS<Arc<Canary>>,
{
    let drops = Arc::new(Mutex::new(Vec::new()));
    let old = canary("old", 1, &drops);
    let mid = canary("mid", 2, &drops);
    let new = canary("new", 3, &drops);

    let shared = ArcSwapAny::<_, S>::from(Arc::clone(&old));

    // Hold 12 guards at once: 8 fast debts + 4 fallback transactions (or 12 fallback
    // transactions with FillFastSlots). Every guard must remain usable and keep its value alive.
    let guards: Vec<Guard<Arc<Canary>, S>> = (0..12).map(|_| shared.load()).collect();
    for g in &guards {
        assert_eq!(g.id, 1, "strategy {}: guard observed a wrong value", strategy_name);
    }
    assert!(
        drops.lock().unwrap().is_empty(),
        "strategy {}: old value dropped while guards are held: {:?}",
        strategy_name,
        *drops.lock().unwrap(),
    );

    // A swap while all guards are alive must help/pay all outstanding debts before the old Arc
    // can be destroyed.
    let prev = shared.swap(Arc::clone(&mid));
    assert_eq!(prev.id, 1);
    assert!(
        drops.lock().unwrap().is_empty(),
        "strategy {}: old value dropped during swap with held guards",
        strategy_name,
    );

    // A fresh load sees the new snapshot.
    let fresh = shared.load();
    assert_eq!(fresh.id, 2);
    drop(fresh);

    // Dropping all old guards still doesn't destroy the old value while `old`/`prev` exist.
    drop(guards);
    assert!(
        drops.lock().unwrap().is_empty(),
        "strategy {}: value dropped too early: {:?}",
        strategy_name,
        *drops.lock().unwrap(),
    );

    drop(prev);
    drop(shared);
    drop(mid);
    drop(old);
    let mut log = drops.lock().unwrap().clone();
    log.sort_by_key(|(_, id)| *id);
    assert_eq!(
        log,
        vec![("old", 1u64), ("mid", 2u64)],
        "strategy {}: wrong drop trace",
        strategy_name,
    );
    assert_eq!(Arc::strong_count(&new), 1, "unused Arc must keep count 1");
}

/// Hypothesis refuted: "publishing a fast debt already pins the linearized value".
///
/// Interleave (reader parked *after* `swap(ptr, SeqCst)` into the slot but before the confirming
/// storage load):
///
/// ```text
/// reader: load ptr, publish debt(ptr)   [parks at FastPreConfirm]
/// writer: swap(mid)  -> pay_all sees & pays the debt, old survives by one extra count
/// reader: resumes, confirm load sees mid -> pays the stale debt itself (fails, already paid),
///         returns a fully owned Arc to the OLD pointer (debt: None)
/// ```
///
/// The returned guard is the old value with `debt = None` (writer bumped the count), and the old
/// value drops exactly once, only after the guard goes away together with every other owner.
#[test]
fn litmus_fast_debt_published_before_writer_swap() {
    let _g = lock();
    let drops = Arc::new(Mutex::new(Vec::new()));
    let old = canary("old", 11, &drops);
    let mid = canary("mid", 12, &drops);
    let shared = Arc::new(ArcSwap::from(Arc::clone(&old)));

    let interleave = Interleave::new();
    interleave.arm(HookPoint::FastPreConfirm, Some(storage_addr(&shared)));

    let worker_done = Arc::new(Barrier::new(2));
    let done = Arc::clone(&worker_done);
    let shared_w = Arc::clone(&shared);

    thread::scope(|scope| {
        scope.spawn(move |_| {
            let guard = shared_w.load();
            done.wait();
            assert_eq!(guard.id, 11, "reader must observe the pre-swap value");
            drop(guard);
        });

        interleave.wait_parked();
        assert_eq!(interleave.parked_on(), Some(HookPoint::FastPreConfirm));

        // The debt is published now. Swap and force the full helping walk while the reader is
        // parked between the two reader-side storage loads.
        let prev = shared.swap(Arc::clone(&mid));
        assert_eq!(prev.id, 11);
        // Old is alive: `old`, `prev`, the storage no longer points to it, and the writer's
        // debt walk bumped one count for the published slot.
        assert_eq!(Arc::strong_count(&old), 3, "writer must have paid the debt");
        assert!(
            drops.lock().unwrap().is_empty(),
            "old value must not drop while reader guard exists: {:?}",
            *drops.lock().unwrap(),
        );

        interleave.release();
        worker_done.wait();

        // The reader returned `old` as a fully owned Arc (debt already paid). Its drop, together
        // with `prev`, still keeps the value alive via `old`.
        assert!(
            drops.lock().unwrap().is_empty(),
            "value must survive until `old` goes: {:?}",
            *drops.lock().unwrap(),
        );
        drop(prev);
        assert!(
            drops.lock().unwrap().is_empty(),
            "drop trace before final owner: {:?}",
            *drops.lock().unwrap(),
        );
    })
    .expect("litmus thread panicked");

    drop(interleave);
    drop(shared);
    drop(mid);
    drop(old);
    let mut log = drops.lock().unwrap().clone();
    log.sort_by_key(|(_, id)| *id);
    assert_eq!(
        log,
        vec![("old", 11u64), ("mid", 12u64)],
        "each old value must drop exactly once",
    );
}

/// The most dangerous counterexample for "debt helping / memory orderings": the collision path of
/// the fallback slot.
///
/// Hypothesis refuted: "a fallback reader always returns the pointer it loaded and pays for that
/// pointer itself".
///
/// Interleave:
///
/// ```text
/// reader: publish generation + active_addr       [parks at FallbackReserved]
/// writer: swap(mid), pay_all observes the generation:
///         - checks active_addr == storage (same ArcSwap),
///         - performs a full load() of the NEW pointer, bumps its count,
///         - CaS(gen -> handover) offers the replacement
/// reader: loads mid as its candidate, confirm sees the handover:
///         - returns the writer-protected mid with NO debt,
///         - pays back its own unused candidate debt (writer had already paid it too -> dec)
/// ```
///
/// If the active-addr check or the SeqCst ordering of generation vs. pointer swap were missing,
/// the writer could either help a reader of a *different* ArcSwap with a dangling address, or the
/// reader could use a stale/freed candidate.
#[test]
fn litmus_helping_collision_writer_offers_replacement() {
    let _g = lock();
    let drops = Arc::new(Mutex::new(Vec::new()));
    let old = canary("old", 21, &drops);
    let mid = canary("mid", 22, &drops);
    let shared = Arc::new(ArcSwap::from(Arc::clone(&old)));

    let interleave = Interleave::new();
    interleave.arm(HookPoint::FallbackReserved, Some(storage_addr(&shared)));

    let worker_done = Arc::new(Barrier::new(2));
    let done = Arc::clone(&worker_done);
    let guard_live = Arc::new(Barrier::new(2));
    let done2 = Arc::clone(&guard_live);
    let guard_gone = Arc::new(Barrier::new(2));
    let done3 = Arc::clone(&guard_gone);
    let shared_w = Arc::clone(&shared);

    thread::scope(|scope| {
        scope.spawn(move |_| {
            // Debt nodes are *thread local*: the worker fills its OWN 8 fast slots so the next
            // load is forced through the helping/fallback transaction.
            let fillers: Vec<_> = (0..8).map(|_| shared_w.load()).collect();
            let guard = shared_w.load();
            done.wait();
            assert_eq!(
                guard.id, 22,
                "helped reader must receive the writer-loaded replacement"
            );
            // Guard is still alive at this barrier: the helped reader owns a real ref count on
            // the writer-provided replacement.
            done2.wait();
            drop(guard);
            done3.wait();
            drop(fillers);
        });

        interleave.wait_parked();
        // Exactly one generation reservation is observed, no stray points from this storage.
        assert!(
            interleave
                .hits()
                .iter()
                .any(|(p, a)| *p == HookPoint::FallbackReserved && *a == storage_addr(&shared)),
            "fallback reservation was not published",
        );

        // The generation reservation is active; the writer takes the collision path and hands
        // over a protected `mid`.
        let prev = shared.swap(Arc::clone(&mid));
        assert_eq!(prev.id, 21);
        // mid: one in storage, one with the writer's handover (held by the parked reader's
        // envelope), plus our own `mid`.
        assert_eq!(
            Arc::strong_count(&mid),
            3,
            "writer must pre-protect the replacement for the reader",
        );
        assert!(
            drops.lock().unwrap().is_empty(),
            "nothing may drop during the collision: {:?}",
            *drops.lock().unwrap(),
        );

        interleave.release();
        worker_done.wait();

        // The reader's guard is still alive here (it waits on the second barrier). Storage +
        // reader's handover-owned Arc + our `mid` => 3.
        assert_eq!(
            Arc::strong_count(&mid),
            3,
            "replacement accounting wrong after handover",
        );
        guard_live.wait();
        // Guard about to drop; after the third barrier it has been dropped.
        guard_gone.wait();
        assert_eq!(
            Arc::strong_count(&mid),
            2,
            "handover Arc must be consumed by the reader's guard drop",
        );
        drop(prev);
    })
    .expect("litmus thread panicked");

    drop(interleave);
    drop(shared);
    drop(mid);
    drop(old);
    let mut log = drops.lock().unwrap().clone();
    log.sort_by_key(|(_, id)| *id);
    assert_eq!(
        log,
        vec![("old", 21u64), ("mid", 22u64)],
        "collision accounting must drop each value exactly once",
    );
}

/// Hypothesis refuted: "CAS whose `current` matched the load will install".
///
/// Interleave: the CAS thread observes equality and parks at `CasObservedEqual` (after the
/// observation point, before the installing `compare_exchange_weak`). A concurrent plain
/// `store` wins the modification order. The CAS must retry and return the actual stored value,
/// and its rejected `new` Arc must not leak into storage nor bump ref counts.
#[test]
fn litmus_cas_loses_between_observation_and_installation() {
    let _g = lock();
    let drops = Arc::new(Mutex::new(Vec::new()));
    let old = canary("old", 31, &drops);
    let incoming = canary("incoming", 32, &drops);
    let racer = canary("racer", 33, &drops);
    let shared = Arc::new(ArcSwap::from(Arc::clone(&old)));

    let interleave = Interleave::new();
    interleave.arm(HookPoint::CasObservedEqual, Some(storage_addr(&shared)));

    let done = Arc::new(Barrier::new(2));
    let done_w = Arc::clone(&done);
    let shared_w = Arc::clone(&shared);
    let old_w = Arc::clone(&old);
    let incoming_w = Arc::clone(&incoming);

    thread::scope(|scope| {
        scope.spawn(move |_| {
            let prev = shared_w.compare_and_swap(&old_w, incoming_w);
            assert_eq!(prev.id, 33, "CAS must observe the winning store, not `old`");
            done_w.wait();
            drop(prev);
        });

        interleave.wait_parked();

        // Plain store wins the modification order in the exact gap between the CAS observation
        // and its installing CAS.
        let stored_prev = shared.swap(Arc::clone(&racer));
        assert_eq!(stored_prev.id, 31);
        drop(stored_prev);

        interleave.release();
        done.wait();

        let winner = shared.load_full();
        assert_eq!(winner.id, 33, "racer value must be the one in storage");
        drop(winner);

        // `incoming` never went into storage: the sole owner is the local on the CAS thread,
        // which is destroyed when `compare_and_swap` unwinds the failed attempt.
        assert_eq!(
            Arc::strong_count(&incoming),
            1,
            "rejected CAS candidate must not leak ref counts",
        );
    })
    .expect("litmus thread panicked");

    drop(interleave);
    drop(shared);
    drop(racer);
    drop(incoming);
    drop(old);
    let mut log = drops.lock().unwrap().clone();
    log.sort_by_key(|(_, id)| *id);
    assert_eq!(
        log,
        vec![("old", 31u64), ("incoming", 32u64), ("racer", 33u64)],
        "CAS loser accounting must be exact",
    );
}

/// Hypothesis refuted: "a writer must help every generation-tagged slot it sees".
///
/// The helping collision path first re-confirms that the reservation targets *this* storage
/// (`active_addr`). A generation published by a reader loading from a *different* `ArcSwap` must
/// be ignored entirely; otherwise the writer would load through (and possibly dereference) an
/// address unrelated to its type.
#[test]
fn litmus_helping_ignores_reservation_for_other_storage() {
    let _g = lock();
    let drops_a = Arc::new(Mutex::<Vec<(&'static str, u64)>>::new(Vec::new()));
    let drops_b = Arc::new(Mutex::<Vec<(&'static str, u64)>>::new(Vec::new()));
    let a_old = canary("a-old", 41, &drops_a);
    let a_new = canary("a-new", 42, &drops_a);
    let b_old = canary("b-old", 43, &drops_b);
    let shared_a = Arc::new(ArcSwap::from(Arc::clone(&a_old)));
    let shared_b = Arc::new(ArcSwap::from(Arc::clone(&b_old)));

    let interleave = Interleave::new();
    // Park the B reader with its generation live for B. A's writer must observe the generation,
    // compare active_addr against A and skip the help.
    interleave.arm(
        HookPoint::FallbackReserved,
        Some(storage_addr_generic(&*shared_b)),
    );

    let reader_release = Arc::new(Barrier::new(2));
    let reader_release_w = Arc::clone(&reader_release);
    let reader_gone = Arc::new(Barrier::new(2));
    let reader_gone_w = Arc::clone(&reader_gone);
    let b_w = Arc::clone(&shared_b);

    thread::scope(|scope| {
        scope.spawn(move |_| {
            // Fill own fast slots, then start a fallback transaction against B.
            let fillers: Vec<_> = (0..8).map(|_| b_w.load()).collect();
            let guard = b_w.load();
            reader_release_w.wait();
            assert_eq!(guard.id, 43, "reader of B must observe B's value");
            drop(guard);
            drop(fillers);
            reader_gone_w.wait();
        });

        interleave.wait_parked();
        // Write A while the generation for B is live on the reader's node. The helping pass for
        // A must not touch B's reservation.
        let prev = shared_a.swap(Arc::clone(&a_new));
        assert_eq!(prev.id, 41);
        // B's value is untouched and still alive.
        assert_eq!(
            Arc::strong_count(&b_old),
            2,
            "writer of A must not influence B's ref counts",
        );
        interleave.release();
        reader_release.wait();
        reader_gone.wait();
        drop(prev);
    })
    .expect("litmus thread panicked");

    drop(interleave);
    drop(shared_a);
    drop(shared_b);
    drop(a_new);
    drop(a_old);
    drop(b_old);
    assert_eq!(
        *drops_b.lock().unwrap(),
        vec![("b-old", 43u64)],
        "B's value drops exactly once",
    );
    let mut log_a = drops_a.lock().unwrap().clone();
    log_a.sort_by_key(|(_, id)| *id);
    assert_eq!(
        log_a,
        vec![("a-old", 41u64), ("a-new", 42u64)],
        "A accounting exact despite foreign reservation",
    );
}

/// Hypothesis refuted: "a fallback reservation necessarily makes the writer help".
///
/// When the reader confirms its generation back to IDLE before the writer scans the node, there
/// is no collision: the reader keeps a normal helping debt and the writer pays it in the ordinary
/// slot scan (writer-side point `WriterNodeAfterHelp` -> slot payment). Old value still drops
/// exactly once.
#[test]
fn litmus_fallback_confirmed_before_writer_pays_slot() {
    let _g = lock();
    let drops = Arc::new(Mutex::new(Vec::new()));
    let old = canary("old", 51, &drops);
    let mid = canary("mid", 52, &drops);
    let shared = Arc::new(ArcSwap::from(Arc::clone(&old)));

    let interleave = Interleave::new();
    interleave.arm(HookPoint::WriterNodeAfterHelp, Some(storage_addr(&shared)));

    let reader_loaded = Arc::new(Barrier::new(3));
    let reader_loaded_w = Arc::clone(&reader_loaded);
    let writer_done = Arc::new(Barrier::new(3));
    let writer_done_w = Arc::clone(&writer_done);
    let shared_r = Arc::clone(&shared);
    let shared_w = Arc::clone(&shared);
    let mid_w = Arc::clone(&mid);
    let reader_loaded_main = Arc::clone(&reader_loaded);
    let writer_done_main = Arc::clone(&writer_done);

    thread::scope(|scope| {
        scope.spawn(move |_| {
            let fillers: Vec<_> = (0..8).map(|_| shared_r.load()).collect();
            let guard = shared_r.load();
            assert_eq!(guard.id, 51);
            reader_loaded_w.wait();
            // Hold the guard (and its helping debt) until the writer has passed the slot scan,
            // i.e. until the writer finishes.
            writer_done_w.wait();
            drop(guard);
            drop(fillers);
        });

        let writer = scope.spawn(move |_| {
            reader_loaded.wait();
            let prev = shared_w.swap(Arc::clone(&mid_w));
            assert_eq!(prev.id, 51);
            drop(prev);
            writer_done.wait();
        });

        reader_loaded_main.wait();
        interleave.wait_parked();
        assert!(
            drops.lock().unwrap().is_empty(),
            "old must survive the helping pass: {:?}",
            *drops.lock().unwrap(),
        );
        interleave.release();
        writer_done_main.wait();
        writer.join().expect("writer panicked");
    })
    .expect("litmus thread panicked");

    drop(interleave);
    drop(shared);
    drop(mid);
    drop(old);
    let mut log = drops.lock().unwrap().clone();
    log.sort_by_key(|(_, id)| *id);
    assert_eq!(
        log,
        vec![("old", 51u64), ("mid", 52u64)],
        "non-colliding fallback debt must be paid by the slot scan",
    );
}


/// Hypothesis refuted: "a parked reader debt can make `ArcSwap::into_inner` return / drop early".
///
/// `into_inner` runs the same `wait_for_readers` debt walk before reconstructing the Arc. A
/// reader parked with a published fast debt must be paid, so the inner value survives until the
/// reader finishes, and the Arc returned by `into_inner` is exactly the old one.
#[test]
fn litmus_into_inner_pays_parked_reader_debt() {
    let _g = lock();
    let drops = Arc::new(Mutex::new(Vec::new()));
    let old = canary("old", 61, &drops);
    let shared = std::mem::ManuallyDrop::new(ArcSwap::from(Arc::clone(&old)));

    let interleave = Interleave::new();
    interleave.arm(HookPoint::FastPreConfirm, Some(storage_addr(&shared)));

    thread::scope(|scope| {
        let shared_ref: &ArcSwap<Canary> = &shared;
        let reader_finished = Arc::new(Barrier::new(2));
        let reader_finished_w = Arc::clone(&reader_finished);
        let reader = scope.spawn(move |_| {
            let guard = shared_ref.load();
            reader_finished_w.wait();
            assert_eq!(guard.id, 61);
            drop(guard);
        });

        interleave.wait_parked();

        // Reclaim ownership while the reader only borrows. `into_inner` runs the debt walk that
        // pays the parked reader's fast slot; the walk completes without waiting for the reader.
        let owned: ArcSwap<Canary> = unsafe { std::ptr::read(shared_ref as *const _ as *const ArcSwap<Canary>) };
        let inner: Arc<Canary> = owned.into_inner();
        assert_eq!(inner.id, 61);
        interleave.wait_pay_all_done();
        interleave.release();
        reader_finished.wait();
        reader.join().expect("reader panicked");
        drop(inner);
    })
    .expect("litmus thread panicked");

    drop(interleave);
    drop(old);
    assert_eq!(
        *drops.lock().unwrap(),
        vec![("old", 61u64)],
        "into_inner must drop the value exactly once after all readers finish",
    );
}

// ================================================================================================
// Executable model of the debt protocol
// ================================================================================================
//
// The real-code tests above can only show the *correct* implementation behaving. To actually
// refute the design hypotheses ("if the algorithm skipped X, the bug would be observable"), this
// model replays scripted microstep histories against both the faithful protocol and deliberately
// broken variants. Each broken variant corresponds to one deleted linearization argument:
//
// * `weak_publish` + `stale_confirm`: reader uses the first storage load as the confirmed value
//   and a writer is allowed not to observe a published debt (missing SeqCst publication / missing
//   second confirming load) -> use-after-free of a protected allocation.
// * `no_helping`: the writer ignores generation reservations in the fallback path -> the reader
//   confirms a candidate the writer has already freed -> use-after-free.
// * `double_pay`: the writer pays a helping-slot debt the reader also resolves -> the old
//   allocation is under-referenced / an extra owner disappears (or the reader double-counts).
// * `cas_force_win`: a CAS installs even after observing a losing store -> the rejected new value
//   leaks an owner and storage holds two unreconciled histories.
mod model {
    use std::collections::HashMap;
    use super::*;

    #[derive(Debug)]
    pub struct Alloc {
        pub phys: i64,
        pub live: bool,
        pub drops: u32,
    }

    #[derive(Clone, Default, Debug)]
    pub struct Faults {
        /// Writer's slot scan may miss a published fast debt, and the reader trusts its stale
        /// first load.
        pub weak_publish: bool,
        /// Reader returns the first pointer even if the second confirming load differs.
        pub stale_confirm: bool,
        /// Writer does not help generation-tagged helping slots.
        pub no_helping: bool,
        /// Helping-slot debt is paid by the writer even though the reader also resolves it.
        pub double_pay: bool,
        /// CAS installs even when it lost the compare-exchange.
        pub cas_force_win: bool,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum GuardKind {
        FastDebt { value: u64, slot: usize, paid_by_writer: bool },
        HelpingDebt { value: u64 },
        Owned { value: u64 },
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum Violation {
        UseAfterFree { alloc: u64, at: &'static str },
        DoubleDrop { alloc: u64 },
        Leak { alloc: u64, phys: i64 },
        NegativeCount { alloc: u64, phys: i64 },
        CasInstalledAfterLosing,
    }

    pub struct World {
        pub faults: Faults,
        pub allocs: HashMap<u64, Alloc>,
        pub storage: Option<u64>,
        pub fast_slots: [Option<u64>; 8],
        pub helping_slot: Option<u64>,
        pub generation_live: bool,
        pub writer_helped_this_node: bool,
        pub helping_candidate_prepaid: bool,
        pub returned_old: Option<u64>,
        pub violations: Vec<Violation>,
    }

    impl World {
        pub fn new(faults: Faults) -> Self {
            World {
                faults,
                allocs: HashMap::new(),
                storage: None,
                fast_slots: [None; 8],
                helping_slot: None,
                generation_live: false,
                writer_helped_this_node: false,
                helping_candidate_prepaid: false,
                returned_old: None,
                violations: Vec::new(),
            }
        }

        pub fn alloc(&mut self, id: u64) {
            self.allocs.insert(
                id,
                Alloc {
                    phys: 1,
                    live: true,
                    drops: 0,
                },
            );
        }

        /// Increments the physical count of a live allocation.
        pub fn inc(&mut self, id: u64, at: &'static str) {
            let a = self.allocs.get_mut(&id).expect("alloc exists");
            if !a.live {
                self.violations
                    .push(Violation::UseAfterFree { alloc: id, at });
            }
            a.phys += 1;
        }

        /// Models an Arc destructor: exactly one drop per allocation, only at count 0.
        pub fn dec(&mut self, id: u64, at: &'static str) {
            let a = self.allocs.get_mut(&id).expect("alloc exists");
            if !a.live {
                self.violations.push(Violation::DoubleDrop { alloc: id });
                return;
            }
            a.phys -= 1;
            if a.phys < 0 {
                self.violations
                    .push(Violation::NegativeCount { alloc: id, phys: a.phys });
            }
            if a.phys == 0 {
                a.live = false;
                a.drops += 1;
            }
        }

        /// A reader touching a guarded value must always find a live allocation.
        pub fn touch(&mut self, id: u64, at: &'static str) {
            let live = self
                .allocs
                .get(&id)
                .map(|a| a.live)
                .expect("allocation known");
            if !live {
                self.violations
                    .push(Violation::UseAfterFree { alloc: id, at });
            }
        }

        pub fn free_fast_slot(&mut self, slot: usize) -> Option<u64> {
            self.fast_slots[slot].take()
        }

        // ---- Reader: fast path microsteps -----------------------------------------------

        /// Reader publishes a fast debt for whatever storage currently holds.
        pub fn reader_fast_publish(&mut self, slot: usize) -> u64 {
            let ptr = self.storage.expect("storage populated");
            self.fast_slots[slot] = Some(ptr);
            ptr
        }

        /// Reader's confirming storage load. Under `stale_confirm` it ignores a concurrent change.
        pub fn reader_fast_confirm(&self, first_ptr: u64) -> u64 {
            if self.faults.stale_confirm {
                first_ptr
            } else {
                self.storage.expect("storage populated")
            }
        }

        /// Completes a fast load, returning the guard the faithful protocol would construct.
        pub fn reader_fast_finish(
            &mut self,
            slot: usize,
            first_ptr: u64,
            at: &'static str,
        ) -> GuardKind {
            let confirmed = self.reader_fast_confirm(first_ptr);
            if confirmed == first_ptr {
                GuardKind::FastDebt {
                    value: confirmed,
                    slot,
                    paid_by_writer: false,
                }
            } else {
                // The slot holds the stale pointer; try to pay it ourselves.
                let held = self.free_fast_slot(slot);
                let _ = held;
                let stale = first_ptr;
                if self.faults.weak_publish {
                    // The writer did NOT see/pay the debt and the stale value may be gone, but the
                    // reader still hands it out as if protected.
                    let _ = at;
                    self.fast_slots[slot] = None;
                    // No matching `inc` ever happened, so this is an unprotected pointer.
                    GuardKind::FastDebt {
                        value: stale,
                        slot,
                        paid_by_writer: true,
                    }
                } else if self.slot_was_writer_paid(slot, stale) {
                    // Writer paid: reader owns the old pointer with no debt.
                    GuardKind::Owned { value: stale }
                } else {
                    // Reader paid it itself: it borrowed without an owned count; must return the
                    // CONFIRMED (new) pointer via a fresh load instead. Faithful code re-enters;
                    // model this as an owned guard on the current value.
                    let cur = self.storage.expect("storage populated");
                    self.inc(cur, "fast-change-self-pay");
                    GuardKind::Owned { value: cur }
                }
            }
        }

        /// Helper the faithful implementation maintains but the weak_publish variant deletes.
        fn slot_was_writer_paid(&self, slot: usize, value: u64) -> bool {
            let _ = (slot, value);
            !self.faults.weak_publish
        }

        // ---- Reader: fallback/helping microsteps ----------------------------------------

        pub fn reader_fallback_reserve(&mut self) {
            self.generation_live = true;
        }

        pub fn reader_fallback_load_candidate(&self) -> u64 {
            self.storage.expect("storage populated")
        }

        /// Reader attempts to confirm. Returns the guard: either a helping debt, or (when the
        /// writer offered a replacement / paid the candidate) an owned pointer.
        pub fn reader_fallback_confirm(&mut self, candidate: u64) -> GuardKind {
            self.generation_live = false;
            // The slot first receives the candidate (confirm's slot swap).
            self.helping_slot = Some(candidate);
            let writer_offered = self.writer_helped_this_node;
            if writer_offered {
                // Writer loaded the current pointer, pre-protected it and handed it over. The
                // reader pays back the candidate debt; if the writer already paid it too (faithful
                // pay_all scans the helping slot), the reader cancels without dec.
                let current = self.storage.expect("storage populated");
                let already_paid = self.helping_candidate_prepaid;
                self.helping_slot = None;
                if !already_paid {
                    // Reader resolves an unowned candidate slot itself -> no owned count to move.
                }
                return GuardKind::Owned { value: current };
            }
            let value = self.helping_slot.take().expect("candidate in slot");
            GuardKind::HelpingDebt { value }
        }

        // ---- Writer microsteps -----------------------------------------------------------

        pub fn writer_swap(&mut self, new: u64, at: &'static str) -> Option<u64> {
            let old = self.storage.replace(new);
            self.writer_pay_all(old, at);
            old
        }

        /// Faithful debt walk: help a live generation for this storage, then pay every fast and
        /// the helping slot matching `old`.
        pub fn writer_pay_all(&mut self, old: Option<u64>, at: &'static str) {
            let old = match old {
                Some(v) => v,
                None => return,
            };
            // Helping collision: a live generation is observed by the faithful writer.
            if self.generation_live && !self.faults.no_helping {
                // Full load of the current (new) pointer, bump its count, offer it.
                let current = self.storage.expect("storage populated");
                self.inc(current, "writer-help-replacement");
                self.writer_helped_this_node = true;
            }
            // Pay fast slots.
            for slot in 0..8 {
                if self.fast_slots[slot] == Some(old) && !self.faults.weak_publish {
                    self.fast_slots[slot] = None;
                    self.inc(old, "writer-pay-fast");
                }
            }
            // Pay the helping slot if it already carries the old candidate.
            if self.helping_slot == Some(old) {
                if self.faults.double_pay {
                    // Broken: bump twice for one debt.
                    self.inc(old, "writer-pay-helping-1");
                    self.inc(old, "writer-pay-helping-2");
                } else {
                    self.inc(old, "writer-pay-helping");
                }
                self.helping_slot = None;
                self.helping_candidate_prepaid = true;
            }

            // `swap` hands the old pointer back to the caller as an owned Arc. The script
            // releases it with `consume_returned_old`; in the broken weak-publication case the
            // old allocation may already be referenced by an unprotected reader guard.
            self.returned_old = Some(old);
        }

        /// Script drops the Arc returned by `swap`.
        pub fn consume_returned_old(&mut self, at: &'static str) {
            if let Some(old) = self.returned_old.take() {
                self.dec(old, at);
            }
        }

        // ---- Guard drop ------------------------------------------------------------------

        pub fn drop_guard(&mut self, guard: GuardKind, at: &'static str) {
            match guard {
                GuardKind::Owned { value } => self.dec(value, at),
                GuardKind::FastDebt {
                    value,
                    slot,
                    paid_by_writer,
                } => {
                    if paid_by_writer {
                        // Debt already converted to an owned count at helping time.
                        self.dec(value, at);
                    } else if self.fast_slots[slot] == Some(value) {
                        self.fast_slots[slot] = None;
                    } else {
                        // Slot already cleared by the writer: the guard carries a real count.
                        self.dec(value, at);
                    }
                }
                GuardKind::HelpingDebt { value } => {
                    if self.helping_slot == Some(value) {
                        self.helping_slot = None;
                    } else {
                        self.dec(value, at);
                    }
                }
            }
        }

        // ---- CAS -------------------------------------------------------------------------

        pub fn cas(&mut self, observed_old: u64, current: u64, new: u64) -> (u64, GuardKind) {
            // Observation point already happened at `observed_old`. The installation point:
            let actual = self.storage.expect("storage populated");
            if actual == current && observed_old == current {
                let old = self.writer_swap(new, "cas-win");
                let value = old.expect("old value");
                (value, GuardKind::Owned { value })
            } else if self.faults.cas_force_win {
                // Broken: install `new` despite losing, and keep acting on the observed pointer.
                let displaced = self.storage.replace(new);
                self.violations.push(Violation::CasInstalledAfterLosing);
                // The forced install pins an owner for `new`; the caller believes the CAS failed
                // and drops its `new` Arc as well, leaving an unreconciled owner.
                self.inc(new, "cas-forced-leak");
                let returned = actual;
                let _ = observed_old;
                if let Some(d) = displaced {
                    // The displaced racer value keeps its own owner (held by the racing writer);
                    // do not drop here.
                    let _ = d;
                }
                (returned, GuardKind::Owned { value: returned })
            } else {
                // Correct loser: retry-equivalent returns the actual value, owned.
                self.inc(actual, "cas-loser-return");
                (actual, GuardKind::Owned { value: actual })
            }
        }

        /// End-of-history check: every live allocation must be reclaimed exactly once.
        pub fn finalize(&mut self) -> Vec<Violation> {
            let ids: Vec<u64> = self.allocs.keys().copied().collect();
            for id in ids {
                // Drop the storage owner and any outstanding modeled owners by driving counts
                // down to zero; the drop log then proves exactly-once.
                if self.storage == Some(id) {
                    self.dec(id, "finalize-storage");
                    self.storage = None;
                }
                let a = self.allocs.get_mut(&id).unwrap();
                if a.live && a.phys == 0 {
                    a.live = false;
                    a.drops += 1;
                }
                let a = self.allocs.get(&id).unwrap();
                if a.drops != 1 {
                    self.violations.push(Violation::Leak {
                        alloc: id,
                        phys: a.phys,
                    });
                }
            }
            std::mem::take(&mut self.violations)
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Executable refutations: scripted histories replayed against correct vs. broken protocols
// ------------------------------------------------------------------------------------------------

use model::{Faults, GuardKind, Violation, World};

/// Faithful protocol + the fast-debt race script: no violations, exact drops.
#[test]
fn model_fast_debt_race_is_safe() {
    let mut w = World::new(Faults::default());
    w.alloc(1);
    w.storage = Some(1);
    let first = w.reader_fast_publish(0);
    // Writer swaps while the debt is published.
    w.alloc(2);
    w.writer_swap(2, "swap-while-fast-debt");
    let g = w.reader_fast_finish(0, first, "reader-confirms");
    w.touch(first, "reader-derefs-guard");
    w.drop_guard(g, "reader-drops");
    let violations = w.finalize();
    assert!(
        violations.is_empty(),
        "faithful fast-debt protocol must be violation-free, got {:?}",
        violations,
    );
}

/// Refutes "debt publication without the SeqCst confirming load is enough". Deleting the
/// publication visibility (`weak_publish`) AND trusting the stale first pointer
/// (`stale_confirm`) lets the reader dereference the old allocation after the writer reclaimed
/// it -> `UseAfterFree`.
#[test]
fn model_refutes_weak_publication_stale_confirm() {
    let mut w = World::new(Faults {
        weak_publish: true,
        stale_confirm: true,
        ..Faults::default()
    });
    w.alloc(1);
    w.storage = Some(1);
    let first = w.reader_fast_publish(0);
    w.alloc(2);
    w.writer_swap(2, "swap-missed-debt");
    let g = w.reader_fast_finish(0, first, "reader-uses-freed-pointer");
    let violations = w.finalize();
    assert!(
        violations
            .iter()
            .any(|v| matches!(v, Violation::UseAfterFree { alloc: 1, .. })),
        "missing publication/confirmation must produce a use-after-free, got {:?}",
        violations,
    );
    // The guard itself carries the stale pointer; dropping it must surface the same class of
    // error rather than silently balancing the counts.
    let mut w2 = World::new(Faults {
        weak_publish: true,
        stale_confirm: true,
        ..Faults::default()
    });
    w2.alloc(11);
    w2.storage = Some(11);
    let first = w2.reader_fast_publish(1);
    w2.alloc(12);
    w2.writer_swap(12, "swap2");
    let g = w2.reader_fast_finish(1, first, "freed");
    w2.drop_guard(g, "drop-freed");
    assert!(w2
        .finalize()
        .iter()
        .any(|v| matches!(v, Violation::UseAfterFree { alloc: 11, .. })
            || matches!(v, Violation::DoubleDrop { alloc: 11 })
            || matches!(v, Violation::NegativeCount { alloc: 11, .. })));
}

/// The most dangerous helping counterexample: if the writer ignores a live generation
/// (`no_helping`), the reader confirms the candidate after the swap and dereferences an
/// allocation the writer already reclaimed -> `UseAfterFree`.
#[test]
fn model_refutes_no_helping_collision() {
    let mut w = World::new(Faults {
        no_helping: true,
        ..Faults::default()
    });
    w.alloc(1);
    w.storage = Some(1);
    w.reader_fallback_reserve();
    w.alloc(2);
    // Swap happens entirely while the generation is live; the broken writer does not help.
    w.writer_swap(2, "swap-without-helping");
    let candidate = w.reader_fallback_load_candidate();
    // In the broken world the candidate is the freed old value; confirming hands it out.
    let g = w.reader_fallback_confirm(candidate);
    w.touch(candidate, "reader-derefs-unhelped-candidate");
    let violations = w.finalize();
    assert!(
        violations
            .iter()
            .any(|v| matches!(v, Violation::UseAfterFree { alloc: 1, .. })),
        "ignoring a generation reservation must cause a use-after-free, got {:?}",
        violations,
    );
    let _ = g;
}

/// Same collision under the faithful helping protocol: the writer hands over a protected current
/// pointer; reader dereferences a live allocation and all drops balance exactly once.
#[test]
fn model_helping_collision_is_safe() {
    let mut w = World::new(Faults::default());
    w.alloc(1);
    w.storage = Some(1);
    w.reader_fallback_reserve();
    w.alloc(2);
    w.writer_swap(2, "swap-helps");
    let candidate = w.reader_fallback_load_candidate();
    let g = w.reader_fallback_confirm(candidate);
    match g {
        GuardKind::Owned { value } => assert_eq!(value, 2, "reader receives the replacement"),
        other => panic!("expected an owned replacement guard, got {:?}", other),
    }
    w.touch(2, "reader-derefs-replacement");
    w.drop_guard(g, "reader-drops-replacement");
    let violations = w.finalize();
    assert!(violations.is_empty(), "got {:?}", violations);
}

/// Refutes "double-paying the helping debt is harmless": the extra bump for one debt leaves an
/// unreconciled owner; finalization reports the imbalance instead of exactly-once drops.
#[test]
fn model_refutes_double_pay_helping_slot() {
    let mut w = World::new(Faults {
        double_pay: true,
        ..Faults::default()
    });
    w.alloc(1);
    w.storage = Some(1);
    // Reader completes the fallback BEFORE the swap: helping debt for 1 is live.
    w.reader_fallback_reserve();
    let candidate = w.reader_fallback_load_candidate();
    let g = w.reader_fallback_confirm(candidate);
    w.alloc(2);
    // Broken writer bumps the helping debt twice.
    w.writer_swap(2, "double-pay");
    w.drop_guard(g, "reader-drops");
    let violations = w.finalize();
    assert!(
        violations
            .iter()
            .any(|v| matches!(v, Violation::Leak { alloc: 1, .. })),
        "double payment must leave an unreconciled owner, got {:?}",
        violations,
    );
}

/// Refutes "CAS may install after losing the observation/install gap". The forced install leaves
/// both the rejected new value with a leaked count and an impossible history.
#[test]
fn model_refutes_cas_force_install_after_losing() {
    let mut w = World::new(Faults {
        cas_force_win: true,
        ..Faults::default()
    });
    w.alloc(1);
    w.alloc(2);
    w.alloc(3);
    w.storage = Some(1);
    // CAS thread observed equality with 1, but another writer installs 2 first.
    w.writer_swap(2, "racing-store");
    let (returned, g) = w.cas(1, 1, 3);
    assert_eq!(returned, 2, "actual storage value is the racer");
    w.drop_guard(g, "cas-drops-returned");
    let violations = w.finalize();
    assert!(
        violations
            .iter()
            .any(|v| matches!(v, Violation::CasInstalledAfterLosing)
                || matches!(v, Violation::Leak { alloc: 3, .. })),
        "forced CAS install must be detectable, got {:?}",
        violations,
    );
}

/// The faithful CAS loser never installs and never leaks.
#[test]
fn model_cas_loser_is_safe() {
    let mut w = World::new(Faults::default());
    w.alloc(1);
    w.alloc(2);
    w.alloc(3);
    w.storage = Some(1);
    w.writer_swap(2, "racing-store");
    let (returned, g) = w.cas(1, 1, 3);
    assert_eq!(returned, 2);
    w.drop_guard(g, "cas-drops");
    // 3 never entered storage; its sole modeled owner goes away at finalization as never-inserted.
    let violations = w.finalize();
    assert!(
        !violations
            .iter()
            .any(|v| matches!(v, Violation::CasInstalledAfterLosing)),
        "faithful CAS must not force an install, got {:?}",
        violations,
    );
}

// ------------------------------------------------------------------------------------------------
// Cross-checking step results against the RwLock<()> reference strategy
// ------------------------------------------------------------------------------------------------

/// A recorded, sequential-history trace. No guard is held across a write (that is *allowed* with
/// the hybrid strategy but would deadlock the RwLock reference model by construction; the audit
/// compares histories that are valid under BOTH strategies).
#[derive(Clone, Copy, Debug)]
enum Step {
    Store(u64),
    Swap(u64),
    LoadExpect(u64),
    Cas(u64, u64, u64), // observed(current), new, expected_returned
}

fn trace() -> Vec<Step> {
    vec![
        Step::LoadExpect(100),
        Step::Store(101),
        Step::LoadExpect(101),
        Step::Swap(102),
        Step::LoadExpect(102),
        Step::Cas(102, 103, 102), // wins, returns 102
        Step::LoadExpect(103),
        Step::Cas(102, 104, 103), // loses (storage is 103), returns 103
        Step::LoadExpect(103),
        Step::Store(105),
        Step::Swap(106),
        Step::LoadExpect(106),
    ]
}

/// Runs the trace against one strategy, returning (observed values, exact drop sequence).
fn run_trace<S>(strategy_name: &str) -> (Vec<u64>, Vec<u64>)
where
    S: Default + Send + Sync + Strategy<Arc<TraceCanary>> + CaS<Arc<TraceCanary>>,
{
    let drops = Arc::new(Mutex::new(Vec::new()));
    let make = |id: u64| trace_canary(id, &drops);
    let shared = ArcSwapAny::<_, S>::from(make(100));
    let mut observed = Vec::new();
    for step in trace() {
        match step {
            Step::Store(v) => shared.store(make(v)),
            Step::Swap(v) => {
                let old = shared.swap(make(v));
                observed.push(old.id);
            }
            Step::LoadExpect(v) => {
                let g = shared.load();
                assert_eq!(
                    g.id, v,
                    "strategy {}: load mismatch at expected {}",
                    strategy_name, v
                );
                observed.push(g.id);
            }
            Step::Cas(current, new, expected) => {
                let cur = make(current);
                let prev = shared.compare_and_swap(&cur, make(new));
                assert_eq!(
                    prev.id, expected,
                    "strategy {}: CAS return mismatch",
                    strategy_name
                );
                observed.push(prev.id);
            }
        }
    }
    drop(shared);
    let log = drops.lock().unwrap().clone();
    (observed, log)
}

#[derive(Debug)]
struct TraceCanary {
    id: u64,
    drops: Arc<Mutex<Vec<u64>>>,
}
impl PartialEq for TraceCanary {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Drop for TraceCanary {
    fn drop(&mut self) {
        self.drops.lock().unwrap().push(self.id);
    }
}
fn trace_canary(id: u64, drops: &Arc<Mutex<Vec<u64>>>) -> Arc<TraceCanary> {
    Arc::new(TraceCanary {
        id,
        drops: Arc::clone(drops),
    })
}

/// The hybrid strategy's per-step results and exact reclamation order must match the simple
/// `RwLock<()>` reference strategy on every sequentially-valid history, including CAS win/loss
/// outcomes. The `FillFastSlots` strategy (every load is a fallback) is included so the helping
/// path is held to the same step semantics.
#[test]
fn crosscheck_step_results_against_rwlock_strategy() {
    let _g = lock();
    let (default_values, default_drops) = run_trace::<DefaultStrategy>("default");
    #[allow(deprecated)]
    let (nofast_values, nofast_drops) =
        run_trace::<arc_swap::strategy::test_strategies::FillFastSlots>("no-fast");
    let (rwlock_values, rwlock_drops) = run_trace::<RwLock<()>>("rwlock");

    assert_eq!(
        default_values, rwlock_values,
        "observed step values must agree: hybrid vs RwLock"
    );
    assert_eq!(
        nofast_values, rwlock_values,
        "observed step values must agree: fallback-only vs RwLock"
    );
    // Exact reclamation: every replaced value drops once, in the same order the reference
    // strategy drops it (RwLock drops synchronously when its write guard releases the old Arc).
    assert_eq!(
        default_drops, rwlock_drops,
        "drop sequence must agree: hybrid vs RwLock"
    );
    assert_eq!(
        nofast_drops, rwlock_drops,
        "drop sequence must agree: fallback-only vs RwLock"
    );
    // Every intermediate value appears exactly once in the drop trace.
    for id in [100u64, 101, 102, 103, 104, 105] {
        assert_eq!(
            rwlock_drops.iter().filter(|&&d| d == id).count(),
            1,
            "value {} must drop exactly once (drop trace {:?})",
            id,
            rwlock_drops
        );
    }
}
