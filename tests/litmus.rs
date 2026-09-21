//! Deterministic-interleaving litmus suite for the audit:
//! "debt helping, guard fallback and CAS linearization points".
//!
//! Every scenario here is a *counter-example harness*: it places one thread at an exact
//! instruction of the reader/writer protocol (via `arc_swap::litmus`) and then asserts the
//! outcome that a naive or weakened implementation would violate. The hypothesis each scenario
//! falsizes is documented on the test. Scenarios are also executed with the `RwLock<()>` test
//! strategy and the `FillFastSlots` (helping-only) configuration so the step results can be cross
//! checked.
//!
//! No sleeps, no network, no host-specific paths: rendez-vous is provided by the crate's own
//! one-shot checkpoints with bounded-deadline panics that dump the counter snapshot.

#![cfg(feature = "internal-test-strategies")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use arc_swap::litmus::{
    self, CP_FALLBACK_CONFIRM, CP_FALLBACK_GEN, CP_FAST_CLAIMED, CP_FAST_CONFIRM, CP_INTO_INNER,
};
use arc_swap::strategy::test_strategies::FillFastSlots;
use arc_swap::{ArcSwapAny, Guard};
use crossbeam_utils::thread;

/// A payload that records how many times it was dropped, keyed by an explicit identity.
#[derive(Debug)]
struct Token {
    id: u32,
    drops: Arc<AtomicUsize>,
}

impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn token(id: u32) -> (Arc<Token>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (Arc::new(Token { id, drops: drops.clone() }), drops)
}

type Hybrid = ArcSwapAny<Arc<Token>, arc_swap::DefaultStrategy>;
type NoFast = ArcSwapAny<Arc<Token>, FillFastSlots>;
type Locked = ArcSwapAny<Arc<Token>, RwLock<()>>;

/// The litmus probes are process-global; serialize scenarios that use them.
static LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn counter(idx: usize) -> u64 {
    litmus::counter(idx)
}

/// Step result of "swap while N guards are alive": drop counters plus surviving strong counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SwapTrace {
    old_dropped_after_swap: usize,
    old_dropped_after_guards: usize,
    old_dropped_final: usize,
    surviving_strong_after: usize,
}

fn run_swap_with_guards(make_guards: usize) -> SwapTrace {
    let (old, old_drops) = token(1);
    let (new, _new_drops) = token(2);
    let shared = Hybrid::from(old.clone());
    let guards: Vec<_> = (0..make_guards).map(|_| shared.load()).collect();
    shared.swap(new);
    let after_swap = old_drops.load(Ordering::SeqCst);
    drop(guards);
    let surviving = shared.load_full();
    drop(old);
    let trace = SwapTrace {
        old_dropped_after_swap: after_swap,
        old_dropped_after_guards: old_drops.load(Ordering::SeqCst),
        old_dropped_final: old_drops.load(Ordering::SeqCst),
        surviving_strong_after: Arc::strong_count(&surviving),
    };
    trace
}

/// Result of the same step sequence under a lock-based strategy.
fn run_swap_with_guards_locked(make_guards: usize) -> SwapTrace {
    let (old, old_drops) = token(101);
    let (new, _) = token(102);
    let shared = Locked::from(old.clone());
    let guards: Vec<_> = (0..make_guards).map(|_| shared.load()).collect();
    shared.swap(new);
    let after_swap = old_drops.load(Ordering::SeqCst);
    drop(guards);
    let surviving = shared.load_full();
    drop(old);
    drop(shared);
    SwapTrace {
        old_dropped_after_swap: after_swap,
        old_dropped_after_guards: old_drops.load(Ordering::SeqCst),
        old_dropped_final: old_drops.load(Ordering::SeqCst),
        surviving_strong_after: Arc::strong_count(&surviving),
    }
}

/// L1: one thread can hold more than eight guards simultaneously.
///
/// Falsizes "readers have only 8 fast slots, so a ninth co-existing guard cannot be protected".
/// The first 8 stay zero-cost debts; later loads must transparently fall back to full Arcs and
/// every guard must still observe a consistent, alive value.
#[test]
fn more_than_eight_guards_same_thread() {
    let _g = lock();
    litmus::reset();
    const GUARDS: usize = 24;
    let (old, old_drops) = token(1);
    let shared = Hybrid::from(old.clone());

    let guards: Vec<_> = (0..GUARDS).map(|_| shared.load()).collect();
    for guard in &guards {
        assert_eq!(guard.id, 1, "guard observed a value it never loaded");
    }
    // Exactly the eight fast slots are debt-protected; every guard beyond them is an owned Arc.
    assert_eq!(counter(litmus::FAST_CONFIRMED), 8);
    assert!(counter(litmus::FALLBACK_CONFIRMED) + counter(litmus::FALLBACK_UPGRADED) >= 1);

    // A swap while every guard is alive must not free the old value.
    let (new, _) = token(2);
    shared.swap(new);
    assert_eq!(old_drops.load(Ordering::SeqCst), 0, "old value dropped under live guards");
    assert!(Arc::strong_count(&old) >= 2, "all live guards observe an alive value");
    drop(guards);
    drop(old);
    assert_eq!(old_drops.load(Ordering::SeqCst), 1, "old value must drop exactly once");

    // Vacating a fallback-upgraded guard frees no slot (it was never a debt), while vacating a
    // fast guard does: a new load then reuses a fast slot without another fallback.
    let baseline = counter(litmus::FALLBACK_CONFIRMED);
    let _reuse = shared.load();
    assert_eq!(counter(litmus::FAST_CONFIRMED), 9);
    assert_eq!(counter(litmus::FALLBACK_CONFIRMED), baseline);
}

/// L1 cross-check: forcing every load through the helping fallback (`FillFastSlots`) yields the
/// same externally visible accounting with zero fast-slot confirmations.
#[test]
fn more_than_eight_guards_helping_only() {
    let _g = lock();
    litmus::reset();
    const GUARDS: usize = 16;
    let (old, old_drops) = token(11);
    let shared = NoFast::from(old.clone());

    let guards: Vec<_> = (0..GUARDS).map(|_| shared.load()).collect();
    for guard in &guards {
        assert_eq!(guard.id, 11);
    }
    assert_eq!(counter(litmus::FAST_CONFIRMED), 0);
    assert_eq!(
        counter(litmus::FALLBACK_CONFIRMED) + counter(litmus::FALLBACK_UPGRADED),
        GUARDS as u64
    );
    let (new, new_drops) = token(12);
    shared.swap(new.clone());
    assert_eq!(old_drops.load(Ordering::SeqCst), 0);
    drop(guards);
    drop(old);
    assert_eq!(old_drops.load(Ordering::SeqCst), 1);
    drop(new);
    drop(shared);
    assert_eq!(new_drops.load(Ordering::SeqCst), 1);
}

/// L1 cross-check with the `RwLock<()>` strategy: same step sequence, same final ownership
/// accounting, only the timing of the single drop differs.
#[test]
fn more_than_eight_guards_rwlock_crosscheck() {
    let _g = lock();
    let trace_hybrid_fast = run_swap_with_guards(3);
    let trace_hybrid_overflow = run_swap_with_guards(12);
    let trace_locked_fast = run_swap_with_guards_locked(3);
    let trace_locked_overflow = run_swap_with_guards_locked(12);

    // While guards are alive no strategy may free the old value, regardless of slot mechanics.
    for trace in [
        trace_hybrid_fast,
        trace_hybrid_overflow,
        trace_locked_fast,
        trace_locked_overflow,
    ] {
        assert_eq!(trace.old_dropped_after_swap, 0, "no free while guards are live");
        assert_eq!(trace.old_dropped_final, 1, "exactly one drop of the old value");
        assert!(trace.surviving_strong_after >= 1);
    }
    // The RwLock reader always takes a full Arc; hybrid fast readers used debts, so after the
    // swap the surviving value has at least as many live owners under the lock cross-check.
    assert_eq!(trace_locked_fast.surviving_strong_after, 1);
    assert!(trace_hybrid_fast.surviving_strong_after >= trace_locked_fast.surviving_strong_after);
}

/// L2: helping collision at the *generation publication* window.
///
/// Falsizes "a writer that finds a half-published reservation must block until the reader
/// finishes, or may skip the node". Deterministic schedule: the reader parks with a tagged
/// generation and no debt yet; the writer must finish the reservation *for* the reader (bump the
/// ref count, hand over a protected pointer), and it must do so without waiting.
#[test]
fn fallback_help_collision_at_generation() {
    let _g = lock();
    litmus::reset();
    let (old, old_drops) = token(21);
    let shared = Arc::new(NoFast::from(old.clone()));

    litmus::arm(CP_FALLBACK_GEN);
    let observed = thread::scope(|scope| {
        let shared = &shared;
        let reader = scope.spawn(move |_| shared.load());
        litmus::wait_fired(CP_FALLBACK_GEN);

        // The reader has published its generation and active address but has not loaded a
        // candidate nor installed any debt. A blocking writer would deadlock here; a skipping
        // writer would free `old` out from under the reader.
        let (new, _) = token(22);
        shared.swap(new);
        assert_eq!(
            counter(litmus::HELP_SUCCESS),
            1,
            "writer must complete the reader's reservation itself"
        );
        assert_eq!(old_drops.load(Ordering::SeqCst), 0);

        litmus::release(CP_FALLBACK_GEN);
        reader.join().unwrap()
    })
    .unwrap();

    // The reader was handed the *new* value, already protected; its own stale debt was paid back.
    assert_eq!(observed.id, 22);
    assert_eq!(counter(litmus::FALLBACK_UPGRADED), 1);
    drop(observed);
    drop(old);
    assert_eq!(old_drops.load(Ordering::SeqCst), 1);
}

/// L3: fast slot whose debt is paid by the writer between the two pointer reads.
///
/// Falsizes "once a fast slot is claimed, the reader owns that debt and the writer cannot touch
/// it". Here the writer wins the window: by the time the reader re-reads the storage the slot is
/// already paid, so the reader silently becomes the owner of a full Arc instead of holding a debt.
#[test]
fn fast_debt_already_paid_between_reads() {
    let _g = lock();
    litmus::reset();
    let (old, old_drops) = token(31);
    let shared = Arc::new(Hybrid::from(old.clone()));

    litmus::arm(CP_FAST_CONFIRM);
    let observed = thread::scope(|scope| {
        let shared = &shared;
        let reader = scope.spawn(move |_| shared.load());
        litmus::wait_fired(CP_FAST_CONFIRM);

        let (new, _) = token(32);
        shared.swap(new);
        // The writer traversed the node and paid the debt that protects the reader's candidate.
        assert!(counter(litmus::PAY_SLOT) >= 1);
        assert_eq!(old_drops.load(Ordering::SeqCst), 0);

        litmus::release(CP_FAST_CONFIRM);
        reader.join().unwrap()
    })
    .unwrap();

    // The reader's confirm load loses the race; its debt was already paid, so it keeps the old
    // pointer as an *owned* Arc (debt == None) and the old value dies exactly once, with it.
    assert_eq!(observed.id, 31);
    assert_eq!(counter(litmus::FAST_ALREADY_PAID), 1);
    assert_eq!(counter(litmus::FAST_CONFIRMED), 0);
    drop(observed);
    drop(old);
    assert_eq!(old_drops.load(Ordering::SeqCst), 1);
}

/// L4: helping collision in the narrowest window of `confirm`.
///
/// The reader's debt is *already installed* in the helping slot while the control still
/// advertises the generation. The writer therefore does two things to the same reader: it offers
/// a replacement through the control and its subsequent `pay_all` may pay the installed slot too.
/// Falsizes two symmetric mistakes: "the helped reader should dec twice" (double free) and
/// "the paid slot needs no payment because help already happened" (leak). Repeated 8 times, each
/// rendez-vous must be consumed exactly once.
#[test]
fn fallback_help_collision_at_confirm_window() {
    let _g = lock();
    const ROUNDS: u64 = 8;
    for round in 0..ROUNDS {
        litmus::reset();
        let (old, old_drops) = token(40 + round as u32);
        let shared = Arc::new(NoFast::from(old));

        litmus::arm(CP_FALLBACK_CONFIRM);
        let observed = thread::scope(|scope| {
            let shared = &shared;
            let reader = scope.spawn(move |_| shared.load());
            litmus::wait_fired(CP_FALLBACK_CONFIRM);

            let (new, _) = token(60 + round as u32);
            shared.swap(new);
            // Exactly one round trip through the help hand-over.
            assert_eq!(counter(litmus::HELP_SUCCESS), 1);
            // The old value is reachable through the reader's parked debt, so it must survive.
            assert_eq!(old_drops.load(Ordering::SeqCst), 0);

            litmus::release(CP_FALLBACK_CONFIRM);
            reader.join().unwrap()
        })
        .unwrap();

        // The reader discovers the replacement, aborts its own debt and keeps the new value.
        assert_eq!(observed.id, 60 + round as u32);
        assert_eq!(counter(litmus::FALLBACK_UPGRADED), 1);
        drop(observed);
        assert_eq!(
            old_drops.load(Ordering::SeqCst),
            1,
            "round {round}: old value must drop exactly once, not zero (leak) or twice (double dec)"
        );
    }
}

/// L5: the same collision window observed at the *generation publication* point too: writer
/// liveness is part of the contract. This parks before the candidate load and asserts the writer
/// returns (helps) instead of waiting for the reader.
#[test]
fn fallback_writer_never_waits_for_reader() {
    let _g = lock();
    litmus::reset();
    let (old, old_drops) = token(51);
    let shared = Arc::new(NoFast::from(old));

    litmus::arm(CP_FALLBACK_GEN);
    thread::scope(|scope| {
        let shared = &shared;
        let reader = scope.spawn(move |_| shared.load());
        litmus::wait_fired(CP_FALLBACK_GEN);
        // A bounded rendez-vous: if helping ever blocked, the join below hits the checkpoint
        // deadline and the test fails with the counter snapshot instead of hanging.
        let (new, _) = token(52);
        shared.store(new);
        litmus::release(CP_FALLBACK_GEN);
        let guard = reader.join().unwrap();
        assert_eq!(guard.id, 52);
    })
    .unwrap();
    // The reader protected the new value; the old one had no remaining protector, so it is freed
    // by the store's helping walk exactly once.
    assert_eq!(old_drops.load(Ordering::SeqCst), 1);
}

/// L6: the two guard-drop repayment paths, observed with drop counters.
///
/// Falsizes "dropping a guard always just clears a slot" (would leak the owned Arc after helping)
/// and "dropping a guard always runs an Arc destructor" (would double-dec a live fast debt).
#[test]
fn guard_drop_two_repayment_paths() {
    let _g = lock();
    litmus::reset();
    let (v, v_drops) = token(70);
    let shared = Arc::new(Hybrid::from(v));

    // Quiet path: nobody writes, the debt is returned by the guard itself.
    let quiet = shared.load();
    drop(quiet);
    assert_eq!(counter(litmus::GUARD_DROP_DEBT_PAID), 1);
    assert_eq!(counter(litmus::GUARD_DROP_ALREADY_PAID), 0);
    assert_eq!(v_drops.load(Ordering::SeqCst), 0);

    // Helped path: a confirmed fast guard is bumped by a writer; its drop owns the Arc.
    let (new, new_drops) = token(71);
    let before_second_load = counter(litmus::FAST_CONFIRMED);
    let guard = shared.load();
    assert_eq!(counter(litmus::FAST_CONFIRMED), before_second_load + 1);
    shared.swap(new);
    assert_eq!(v_drops.load(Ordering::SeqCst), 0, "writer must not free a guarded value");
    drop(shared);
    assert_eq!(v_drops.load(Ordering::SeqCst), 0, "the helped guard still keeps the value alive");
    drop(guard);
    assert_eq!(counter(litmus::GUARD_DROP_ALREADY_PAID), 1);
    // The test moved its only `v` clone into the storage, so the helped guard's drop is the
    // unique final drop of the old value.
    assert_eq!(v_drops.load(Ordering::SeqCst), 1, "exactly one final drop, from the helped guard");
    assert_eq!(new_drops.load(Ordering::SeqCst), 1);
}

/// L7: `Guard::into_inner` either cancels a live debt with one increment or, when a writer already
/// paid it, rolls the speculative increment back. Falsizes "into_inner can unconditionally clone"
/// (over-count), "the debt is always still ours" (under-count/use-after-free) and "skip the
/// rollback when the pay fails" (leak).
#[test]
fn guard_into_inner_repayment_paths() {
    let _g = lock();
    litmus::reset();
    let (v, v_drops) = token(80);
    let shared = Arc::new(Hybrid::from(v.clone()));

    // Live-debt path: one inc transfers the protection to the produced Arc.
    let owned = {
        let guard = shared.load();
        assert_eq!(Arc::strong_count(&v), 2);
        Guard::into_inner(guard)
    };
    assert_eq!(counter(litmus::INTO_INNER_DEBT_PAID), 1);
    assert_eq!(Arc::strong_count(&v), 2);
    drop(owned);
    assert_eq!(Arc::strong_count(&v), 1);

    // Already-paid path: park inside into_inner between the speculative inc and the debt pay,
    // let a writer pay the slot, then the rollback must remove exactly the speculative count.
    litmus::arm(CP_INTO_INNER);
    thread::scope(|scope| {
        let shared = &shared;
        let upgrader = scope.spawn(move |_| {
            let guard = shared.load();
            Guard::into_inner(guard)
        });
        litmus::wait_fired(CP_INTO_INNER);
        // The upgrader holds the value with a speculative extra count; the stored value is
        // replaced and the writer pays every remaining debt (but this slot is still claimed).
        let (new, _) = token(81);
        let before = Arc::strong_count(&v);
        shared.swap(new);
        let during = Arc::strong_count(&v);
        assert_eq!(during, before, "writer cannot pay the still-in-flight slot");
        litmus::release(CP_INTO_INNER);
        let owned = upgrader.join().unwrap();
        // The upgrader's own pay failed (writer paid first); the rollback cancels its inc.
        assert_eq!(counter(litmus::INTO_INNER_ALREADY_PAID), 1);
        assert_eq!(Arc::strong_count(&v), 2, "speculative count rolled back, writer's bump retained");
        drop(owned);
        assert_eq!(Arc::strong_count(&v), 1);
        drop(v);
        assert_eq!(v_drops.load(Ordering::SeqCst), 1);
    })
    .unwrap();
}

/// L8: successful CAS still runs `wait_for_readers`: an outstanding fast guard on the previous
/// value must be converted to an owned Arc by the CAS itself. Falsizes "only swap/ store owe
/// readers help; a winning CAS can finish as soon as the pointer is exchanged".
#[test]
fn cas_success_pays_outstanding_reader() {
    let _g = lock();
    litmus::reset();
    let (old, old_drops) = token(90);
    let shared = Hybrid::from(old.clone());
    assert_eq!(Arc::strong_count(&old), 2);

    let guard = shared.load();
    assert_eq!(Arc::strong_count(&old), 2, "the fast guard holds no ref count");
    let (next, next_drops) = token(91);
    let prev = shared.compare_and_swap(&old, next);
    assert_eq!(prev.id, 90);
    // Accounting after a winning CAS: the reader's outstanding debt was converted to an owned
    // Arc by wait_for_readers; the returned guard is itself protected (fast debt, or an owned Arc
    // when its load had to fall back). Assert the invariant rather than a slot-allocation detail.
    let after_cas = Arc::strong_count(&old);
    assert!((2..=3).contains(&after_cas), "reader guard plus returned guard keep the value alive");
    drop(prev);
    assert_eq!(Arc::strong_count(&old), 1, "only the live (helped) reader guard remains");
    assert_eq!(old_drops.load(Ordering::SeqCst), 0);
    drop(guard);
    drop(old);
    assert_eq!(old_drops.load(Ordering::SeqCst), 1);
    assert_eq!(shared.load().id, 91);
    drop(shared);
    assert_eq!(next_drops.load(Ordering::SeqCst), 1);
}

/// L9: CAS loses when the storage changes after its load. The verdict is linearized at the
/// *failing compare_exchange*, not at the first load: parking the CASer between its two reads and
/// letting an independent swap win must make the CAS report the newer value and drop its own
/// candidate exactly once.
#[test]
fn cas_loses_to_concurrent_swap() {
    let _g = lock();
    litmus::reset();
    let (old, old_drops) = token(100);
    let shared = Arc::new(Hybrid::from(old.clone()));

    // CP_FAST_CLAIMED fires inside the CAS's internal load; after it fires the CAS has only
    // performed the first of its two storage reads.
    litmus::arm(CP_FAST_CLAIMED);
    thread::scope(|scope| {
        let shared = &shared;
        let cas_old = old.clone();
        let caser = scope.spawn(move |_| {
            let candidate = Arc::new(Token {
                id: 102,
                drops: Arc::new(AtomicUsize::new(0)),
            });
            let candidate_drops = candidate.drops.clone();
            let prev = shared.compare_and_swap(&cas_old, candidate);
            (prev, candidate_drops)
        });
        litmus::wait_fired(CP_FAST_CLAIMED);

        let (winner, winner_drops) = token(101);
        shared.swap(winner);
        litmus::release(CP_FAST_CLAIMED);
        let (prev, candidate_drops) = caser.join().unwrap();

        // The CAS verdict reflects the post-swap state: it returns 101 and never installed 102.
        assert_eq!(prev.id, 101);
        assert_eq!(shared.load().id, 101);
        assert_eq!(candidate_drops.load(Ordering::SeqCst), 1, "rejected candidate dropped once");
        // The CAS's losing traversal already helped its own internal load slot; the old value
        // dies with the losing guard chain. The test still owns its `old` clone until dropped.
        drop(prev);
        drop(old);
        drop(shared);
        assert_eq!(old_drops.load(Ordering::SeqCst), 1);
        assert_eq!(winner_drops.load(Ordering::SeqCst), 1);
    })
    .unwrap();
}

/// L10: CAS accounting cross-checked against the `RwLock<()>` strategy with identical steps.
/// Both a successful and a failing CAS must leave the losers' candidates dropped once and the
/// winners with the stored + observable count.
#[test]
fn cas_accounting_crosschecked_with_rwlock() {
    fn profile<S: arc_swap::strategy::Strategy<Arc<Token>> + Default>(id_base: u32) -> (usize, usize, usize)
    where
        S: arc_swap::strategy::CaS<Arc<Token>>,
    {
        let (old, _) = token(id_base);
        let shared = ArcSwapAny::<Arc<Token>, S>::from(old.clone());
        let (win, win_drops) = token(id_base + 1);
        let (lose, lose_drops) = token(id_base + 2);

        let prev = shared.compare_and_swap(&old, win);
        assert_eq!(prev.id, id_base);
        let prev2 = Guard::into_inner(shared.compare_and_swap(&old, lose));
        assert_eq!(prev2.id, id_base + 1);
        let current = shared.load_full();
        let stored_count = Arc::strong_count(&current);
        drop(current);
        drop(prev);
        drop(prev2);
        drop(shared);
        (
            stored_count,
            win_drops.load(Ordering::SeqCst),
            lose_drops.load(Ordering::SeqCst),
        )
    }

    // (strong count while the new value is stored+observed, winner drops after teardown, loser
    // candidate drops immediately). Same step result for both strategies.
    let hybrid = profile::<arc_swap::DefaultStrategy>(110);
    let locked = profile::<RwLock<()>>(120);
    // While `current` is held the winning value has 3 owners (storage, current, prev2 has been
    // folded in); after teardown it drops once; the rejected candidate drops immediately once.
    assert_eq!(hybrid, (3, 1, 1));
    assert_eq!(hybrid, locked);
}

/// L11: dropping the `ArcSwap` itself is linearized through `wait_for_readers` as well: a guard
/// acquired before the drop keeps the value alive across the storage destruction and the value
/// dies exactly once, when the guard goes.
#[test]
fn arcswap_drop_pays_debts_before_dec() {
    let _g = lock();
    litmus::reset();
    let (value, drops) = token(130);
    let shared = Hybrid::from(value);
    let guard = shared.load();

    drop(shared);
    assert_eq!(drops.load(Ordering::SeqCst), 0, "storage destruction must not kill a guarded value");
    assert_eq!(guard.id, 130, "guard remains usable after the ArcSwap is gone");
    drop(guard);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(counter(litmus::PAY_SLOT) >= 1, true);
}

/// L12: `into_inner` (consuming extraction) with a live fast guard likewise pays every debt; the
/// extracted Arc and the guard share the value, which is dropped exactly once overall.
#[test]
fn arcswap_into_inner_pays_debts() {
    let _g = lock();
    litmus::reset();
    let (value, drops) = token(140);
    let shared = Hybrid::from(value);
    let guard = shared.load();

    let extracted = shared.into_inner();
    assert_eq!(extracted.id, 140);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(guard);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(extracted);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
