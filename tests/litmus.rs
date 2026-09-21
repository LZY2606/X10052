//! Deterministic litmus suite for the audit:
//! "debt helping, guard fallback and CAS linearisation points".
//!
//! Every test here is a *controlled interleaving*: threads rendezvous on barriers or on the
//! feature-gated seams in `arc_swap::strategy::test_ctl`, never on sleeps. Each scenario records
//! its schedule as a list of human readable steps (the "trace"), asserts the observable outcome
//! (pointer identity, value and exact drop count) and then replays the *same schedule* against
//! the `RwLock<()>` test strategy, which is the independent reference implementation. Both must
//! produce the same step outcomes.
//!
//! Hypotheses falsified by the scenarios below are listed next to each test; the full audit is in
//! `ANALYSIS.md`. The whole file compiles only with `internal-test-strategies`.

#![cfg(feature = "internal-test-strategies")]
#![allow(deprecated)] // FillFastSlots exists only for this internal litmus suite

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering::*};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use arc_swap::strategy::test_ctl::{self, Window};
use arc_swap::strategy::{CaS, DefaultStrategy, Strategy};
use arc_swap::ArcSwapAny;
use crossbeam_utils::thread;
use once_cell::sync::Lazy;

static SEAM_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Serialises tests that arm the global rendezvous seam. Mirrors `tests/stress.rs`.
fn seam_lock() -> MutexGuard<'static, ()> {
    SEAM_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

type Histogram = Arc<Mutex<HashMap<u64, usize>>>;

/// Payload counting how many times each tagged value is dropped.
struct Tracked {
    tag: u64,
    drops: Arc<AtomicUsize>,
    by_tag: Histogram,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Relaxed);
        *self.by_tag.lock().unwrap().entry(self.tag).or_insert(0) += 1;
    }
}

/// Sparse per-tag drop histogram, so tests can name tags (including sparse ABA-era tags) without
/// preallocating address space.
fn histogram() -> Histogram {
    Arc::new(Mutex::new(HashMap::new()))
}

fn make(tag: u64, drops: &Arc<AtomicUsize>, by_tag: &Histogram) -> Arc<Tracked> {
    Arc::new(Tracked {
        tag,
        drops: Arc::clone(drops),
        by_tag: Arc::clone(by_tag),
    })
}

fn drops_of(by_tag: &Histogram, tag: u64) -> usize {
    by_tag.lock().unwrap().get(&tag).copied().unwrap_or(0)
}

/// One recorded, deterministic step outcome. Traces from different strategies are compared
/// verbatim, which is the cross-strategy oracle required by the audit.
type Trace = Vec<String>;

type Swap<S> = ArcSwapAny<Arc<Tracked>, S>;

// =================================================================================================
// Scenario 1: more than eight guards in one thread
// =================================================================================================
//
// Hypothesis H1 (falsified if this regresses): "a fast debt slot exists for every guard the
// program creates". Only 8 fast slots exist per thread node; the 9th and later guards must enter
// the helping fallback and still keep the pointed-to Arc alive with an exactly balanced ref
// count. We also falsify the converse laziness assumption: releasing the first guards must *not*
// drop the old value (fast slots are paid back, they are not owned Arcs), while every value the
// fallback ever protected is dropped exactly once when all protections end.

fn steps_guard_overflow<S>() -> (Trace, usize, usize)
where
    S: Default + Send + Sync + Strategy<Arc<Tracked>>,
{
    let mut trace = Trace::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared = Swap::<S>::from(make(1, &drops, &by_tag));

    // 16 live guards on the same thread: the first 8 ride fast slots, the rest go through the
    // helping fallback (which ends up owning a real Arc for the protected value).
    let guards: Vec<_> = (0..16).map(|_| shared.load()).collect();
    let all_old = guards.iter().all(|g| g.tag == 1);
    trace.push(format!("all 16 guards observe old tag: {all_old}"));

    // While all 16 guards are alive, the old value must not be dropped by the writer.
    shared.store(make(2, &drops, &by_tag));
    let drops_during = drops.load(Relaxed);
    trace.push(format!("drops while 16 guards held: {drops_during}"));
    assert_eq!(drops_during, 0, "old value dropped while guards still live");

    // Every guard, regardless of which path created it, still reads the *old* value.
    let consistent = guards.iter().all(|g| g.tag == 1);
    trace.push(format!("guards keep pre-store snapshot: {consistent}"));
    assert!(consistent);

    // A fresh load sees the new value and is itself protected.
    let after = shared.load();
    trace.push(format!("post-store load sees new tag: {}", after.tag == 2));
    assert_eq!(after.tag, 2);
    drop(after);

    drop(guards);
    let drops_after_guards = drops.load(Relaxed);
    trace.push(format!(
        "drops after all guards released: {drops_after_guards}"
    ));
    assert_eq!(drops_after_guards, 1, "old value must drop exactly once");

    drop(shared);
    let drops_final = drops.load(Relaxed);
    trace.push(format!("drops after swap destroyed: {drops_final}"));
    assert_eq!(drops_final, 2, "both values dropped exactly once overall");

    (trace, drops_during, drops_final)
}

/// Steps that are boolean predicates and must therefore be `true`. Steps carrying a numeric
/// payload (tag ids, drop counters) are checked by the scenario's own assertions instead.
const BOOLEAN_STEPS: &[&str] = &[
    "observe old tag",
    "keeps pre-store snapshot",
    "sees new tag",
    "dropped while reader parked",
    "reader tag is old or new",
    "matches pointer provenance",
    "preserves the protected value",
    "returns the competing value",
    "returns the previous value",
    "storage keeps the competing value",
    "storage holds the cas value",
    "dropped exactly once",
];

fn assert_trace_outcomes(trace: &Trace) {
    for step in trace {
        if let Some((key, value)) = step.split_once(':') {
            if BOOLEAN_STEPS.iter().any(|pred| key.contains(pred)) {
                assert_eq!(value.trim(), "true", "trace step failed: {step}");
            }
        }
    }
}

#[test]
fn litmus_more_than_eight_guards_default() {
    let (trace, during, final_) = steps_guard_overflow::<DefaultStrategy>();
    assert_eq!(during, 0);
    assert_eq!(final_, 2);
    assert_trace_outcomes(&trace);
}

#[test]
fn litmus_more_than_eight_guards_rwlock_crosscheck() {
    let (trace_lock, during, final_) = steps_guard_overflow::<RwLock<()>>();
    let (trace_hybrid, _, _) = steps_guard_overflow::<DefaultStrategy>();
    assert_eq!(during, 0);
    assert_eq!(final_, 2);
    assert_eq!(
        trace_lock, trace_hybrid,
        "RwLock and Hybrid strategies diverge on the guard overflow schedule"
    );
}

#[cfg(not(miri))]
#[test]
fn litmus_more_than_eight_guards_no_fast_slots_crosscheck() {
    // Fast slots disabled: even the first guard must survive purely on the fallback path.
    let (trace_no_fast, during, final_) =
        steps_guard_overflow::<arc_swap::strategy::test_strategies::FillFastSlots>();
    let (trace_hybrid, _, _) = steps_guard_overflow::<DefaultStrategy>();
    assert_eq!(during, 0);
    assert_eq!(final_, 2);
    assert_eq!(
        trace_no_fast, trace_hybrid,
        "forcing the fallback path must not change observable outcomes"
    );
}

// =================================================================================================
// Scenario 2: reader parked in the helping fallback while a writer replaces the pointer
// =================================================================================================
//
// Hypothesis H2 (falsified if this regresses): "debt slots are acquired atomically with respect
// to writers". They are not: the helping reservation (active address + generation) is published
// before the pointer is loaded and confirmed. The writer must therefore (a) notice the in-flight
// reservation, synthesise an already protected replacement and hand it over (collision path), and
// (b) wait for / pay every debt on the old pointer so the old value cannot be freed underneath a
// parked reader. Two pause windows pin the reader down:
//
//   * AfterReserve: forces the writer's handover/helping collision branch (GEN -> REPLACEMENT).
//   * AfterLoad:     the reader holds only an unconfirmed candidate; the writer's pay_all must
//                    keep that candidate's ref count balanced whether or not the debt is visible
//                    during traversal.
//
// Both windows are fully deterministic: the writer performs its store strictly between the
// reader's `wait_entered` rendezvous and `release`.

fn steps_fallback_writer_collision(window: Window) -> Trace {
    let _guard = seam_lock();
    let mut trace = Trace::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared = Swap::<DefaultStrategy>::from(make(1, &drops, &by_tag));
    let storage_addr = test_ctl::storage_addr_of(&shared);

    thread::scope(|scope| {
        test_ctl::arm(storage_addr, window);
        let reader = scope.spawn(|_| {
            // Fast slots are *thread local*. Fill this reader node's eight fast slots with one
            // guard each on eight other instances, so the `shared.load()` below is guaranteed to
            // enter the helping fallback on this thread.
            let fillers: Vec<Swap<DefaultStrategy>> = (0..8)
                .map(|i| Swap::from(make(100 + i, &drops, &by_tag)))
                .collect();
            let filler_guards: Vec<_> = fillers.iter().map(|f| f.load()).collect();

            // The orchestrator has armed the seam before spawning us. This load parks exactly at
            // the armed point inside the fallback and only returns once the writer releases it.
            let parked = shared.load();
            let reader_tag = parked.tag;
            let reader_addr = Arc::as_ptr(&*parked) as usize;
            let owned = Guard::into_inner(parked);

            (reader_tag, reader_addr, owned, fillers, filler_guards)
        });

        // The writer runs on the orchestrating thread and performs its swap strictly while the
        // reader is parked in the fallback window.
        test_ctl::wait_entered();
        let new_arc = make(2, &drops, &by_tag);
        let new_addr = Arc::as_ptr(&new_arc) as usize;
        let old = shared.swap(new_arc); // linearisation point + wait_for_readers
        let drops_after_swap = drops.load(Relaxed);
        trace.push(format!(
            "old value dropped while reader parked: {}",
            drops_after_swap == 0
        ));
        assert_eq!(old.tag, 1);
        assert_eq!(drops_after_swap, 0, "old value freed under a parked reader");
        test_ctl::release();

        let (reader_tag, reader_addr, owned, _fillers, _filler_guards) = reader.join().unwrap();

        // The reader must finish with a valid pointer: either the old value it protected itself
        // or the writer's handover replacement.
        let tag_plausible = reader_tag == 1 || reader_tag == 2;
        trace.push(format!("reader tag is old or new: {tag_plausible}"));
        assert!(tag_plausible, "reader observed invalid tag {}", reader_tag);

        // Pointer provenance check (the audit core): tag and address must name the *same*
        // object. Dereferencing parked.tag above already proves the memory was alive when the
        // reader resumed; this proves it was the correct object.
        let addr_consistent = match reader_tag {
            1 => reader_addr == Arc::as_ptr(&old) as usize,
            _ => reader_addr == new_addr,
        };
        trace.push(format!(
            "reader tag matches pointer provenance: {addr_consistent}"
        ));
        assert!(
            addr_consistent,
            "reader tag {} mismatches pointer {:#x}",
            reader_tag, reader_addr
        );

        let owned_ok = owned.tag == reader_tag;
        trace.push(format!(
            "into_inner preserves the protected value: {owned_ok}"
        ));
        assert!(owned_ok);
        drop(owned);
        drop(old);
    })
    .unwrap();

    // Reader-local fillers are gone with its scope. Now only the two tracked values remain, both
    // of which must be dropped exactly once when the swap dies.
    drop(shared);
    let drops_final = drops.load(Relaxed);
    trace.push(format!(
        "every allocation dropped exactly once: {}",
        drops_final == 10
    ));
    assert_eq!(
        drops_final, 10,
        "expected 8 filler + old + new drops, got {drops_final}"
    );
    let _ = &by_tag;

    trace
}

use arc_swap::Guard;

#[test]
fn litmus_fallback_collision_after_reserve() {
    let trace = steps_fallback_writer_collision(Window::AfterReserve);
    assert_trace_outcomes(&trace);
}

#[test]
fn litmus_after_reserve_deterministically_enters_handover() {
    let _ = steps_fallback_writer_collision(Window::AfterReserve);
    let (gen_seen, handover_ok) = test_ctl::helper_counters();
    assert!(
        gen_seen >= 1,
        "writer never observed the parked reader generation"
    );
    assert!(
        handover_ok >= 1,
        "writer never completed the handover (collision path not taken)"
    );
}

#[test]
fn litmus_fallback_collision_after_load() {
    let trace = steps_fallback_writer_collision(Window::AfterLoad);
    assert_trace_outcomes(&trace);
}

/// RwLock reference schedule for the same logical interleaving. The RwLock strategy has no
/// helping protocol; its reader simply takes a read lock. The store therefore happens strictly
/// before or after the reader's load, but both outcomes (old tag 1, new tag 2) are legal for the
/// hybrid strategy as well. This checks the *oracle* the hybrid collision paths must satisfy:
/// exactly one value, alive, matching provenance, each allocation dropped once.
#[test]
fn litmus_fallback_collision_rwlock_oracle() {
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared = Swap::<RwLock<()>>::from(make(1, &drops, &by_tag));
    let outcome = thread::scope(|scope| {
        let reader = scope.spawn(|_| {
            let g = shared.load();
            let tag = g.tag;
            let addr = Arc::as_ptr(&*g) as usize;
            (tag, addr, g)
        });
        let new_arc = make(2, &drops, &by_tag);
        let new_addr = Arc::as_ptr(&new_arc) as usize;
        let old = shared.swap(new_arc);
        let (tag, addr, g) = reader.join().unwrap();
        let provenance = match tag {
            1 => addr == Arc::as_ptr(&old) as usize,
            _ => addr == new_addr && tag == 2,
        };
        assert!(provenance, "rwlock reader tag/provenance mismatch");
        drop(g);
        drop(old);
        tag
    })
    .unwrap();
    drop(shared);
    assert!(outcome == 1 || outcome == 2);
    assert_eq!(drops.load(Relaxed), 2);
}

// =================================================================================================
// Scenario 3: a compare_and_swap whose internal load collides with a competing writer
// =================================================================================================
//
// Hypothesis H3 (falsified if this regresses): "the CAS linearisation point is only the
// compare_exchange". It is not sufficient: the CAS first performs a full `load` (to protect the
// old value while it works), then `compare_exchange`, then `wait_for_readers`. If a competing
// swap lands between that load and the compare_exchange, two things must both hold:
//
//   * the CAS must *fail* and return the value currently in storage (never the stale one), and
//   * the ref count of every pointer along the way must stay balanced: the rejected `new` is
//     dropped exactly once (it never entered storage), the displaced `old` exactly once.
//
// Conversely, without a competing write the CAS must succeed and still pay the debt its own
// protected load created (that debt lives in the writer's *own* thread node).
//
// The same reader seam controls the CAS too: `compare_and_swap` calls the strategy `load`
// internally, so filling the CAS thread's fast slots and arming the seam parks the CAS precisely
// between its candidate load and its confirmation.

/// Outcome ledger shared by the deterministic hybrid schedule and the ordered RwLock schedule.
fn cas_steps<S>(preempt: bool, seam: bool) -> Trace
where
    S: CaS<Arc<Tracked>> + Default + Send + Sync + Strategy<Arc<Tracked>>,
{
    // All seam-using schedules share one global rendezvous; serialise them. The reference
    // schedules (seam == false) are also run inside this test, but not here.
    let _seam_guard = if seam { Some(seam_lock()) } else { None };
    let mut trace = Trace::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared = Swap::<S>::from(make(1, &drops, &by_tag));
    let storage_addr = test_ctl::storage_addr_of(&shared);
    let expected_old = {
        let g = shared.load_full();
        Arc::clone(&g)
    };
    let cas_new = make(3, &drops, &by_tag);

    if seam {
        test_ctl::arm(storage_addr, Window::AfterLoad);
    }

    // Filler allocations exist only in the deterministic hybrid schedule, to force the CAS
    // internal load onto the helping fallback. In the seam-less reference schedules (RwLock, and
    // the un-armed hybrid run) they are omitted: RwLock fillers would deadlock the writer, and the
    // strict spawn/join order below already fixes the history.
    let filler_allocations = if seam { 8 } else { 0 };
    thread::scope(|scope| {
        let cas = if seam {
            scope.spawn(|_| {
                // Fill this thread's eight fast slots so the CAS internal load uses the helping
                // fallback and parks at the armed window.
                let fillers: Vec<Swap<S>> = (0..8)
                    .map(|i| Swap::from(make(100 + i, &drops, &by_tag)))
                    .collect();
                let filler_guards: Vec<_> = fillers.iter().map(|f| f.load()).collect();
                let returned = shared.compare_and_swap(&expected_old, cas_new);
                let tag = returned.tag;
                drop(returned);
                drop(filler_guards);
                drop(fillers);
                tag
            })
        } else {
            // Reference schedule: for the preempt case, the swap must be complete before the CAS
            // starts, so run the competing write first and only then spawn the CAS.
            if preempt {
                let mid = make(2, &drops, &by_tag);
                let displaced = shared.swap(mid);
                trace.push(format!("competing swap displaced tag: {}", displaced.tag));
                assert_eq!(displaced.tag, 1);
                drop(displaced);
            }
            scope.spawn(|_| {
                let returned = shared.compare_and_swap(&expected_old, cas_new);
                let tag = returned.tag;
                drop(returned);
                tag
            })
        };

        if seam {
            test_ctl::wait_entered();
        }
        if preempt && seam {
            // Competing writer linearises exactly while the CAS candidate is unconfirmed.
            let mid = make(2, &drops, &by_tag);
            let displaced = shared.swap(mid);
            trace.push(format!("competing swap displaced tag: {}", displaced.tag));
            assert_eq!(displaced.tag, 1);
            drop(displaced);
        }
        if seam {
            test_ctl::release();
        }

        let returned_tag = cas.join().unwrap();
        if preempt {
            trace.push(format!(
                "cas returns the competing value: {}",
                returned_tag == 2
            ));
            assert_eq!(returned_tag, 2, "lost-update CAS must return the winner");
        } else {
            trace.push(format!(
                "cas returns the previous value: {}",
                returned_tag == 1
            ));
            assert_eq!(returned_tag, 1);
        }
    })
    .unwrap();

    // The comparison handle `expected_old` is a genuine strong reference to tag 1; release it
    // before settling the ledger, otherwise tag 1 legitimately stays alive and masks a leak or
    // double-drop in the CAS bookkeeping under audit.
    drop(expected_old);

    let current = shared.load_full();
    if preempt {
        trace.push(format!(
            "storage keeps the competing value: {}",
            current.tag == 2
        ));
        assert_eq!(current.tag, 2);
    } else {
        trace.push(format!("storage holds the cas value: {}", current.tag == 3));
        assert_eq!(current.tag, 3);
    }
    // Filler allocations, plus tag 1 always, plus tag 3 always; the preempt case additionally
    // creates the competing winner tag 2.
    let total_expected = filler_allocations + 2 + usize::from(preempt);
    drop(current);
    drop(shared);
    let total = drops.load(Relaxed);
    trace.push(format!(
        "every value dropped exactly once: {}",
        total == total_expected
    ));
    assert_eq!(
        total,
        total_expected,
        "CAS ledger unbalanced (got {}, expected {}), per-tag: {:?}",
        total,
        total_expected,
        by_tag
            .lock()
            .unwrap()
            .iter()
            .map(|(tag, count)| (*tag, *count))
            .collect::<Vec<_>>()
    );

    trace
}

#[test]
fn litmus_cas_loses_to_swap_default() {
    let trace = cas_steps::<DefaultStrategy>(true, true);
    assert_trace_outcomes(&trace);
}

#[test]
fn litmus_cas_succeeds_default() {
    let trace = cas_steps::<DefaultStrategy>(false, true);
    assert_trace_outcomes(&trace);
}

#[test]
fn litmus_cas_contention_rwlock_crosscheck() {
    // Reference schedule on RwLock: in the preempt case the swap is ordered strictly before the
    // CAS acquires its write lock, which is the same external history the hybrid collision must
    // be equivalent to. No seam exists for RwLock, so it runs in the equivalent strict order.
    let lost_lock = cas_steps::<RwLock<()>>(true, false);
    let lost_hybrid = cas_steps::<DefaultStrategy>(true, true);
    assert_eq!(
        lost_lock, lost_hybrid,
        "lost-update CAS schedules differ between RwLock and Hybrid"
    );
    let won_lock = cas_steps::<RwLock<()>>(false, false);
    let won_hybrid = cas_steps::<DefaultStrategy>(false, true);
    assert_eq!(
        won_lock, won_hybrid,
        "successful CAS schedules differ between RwLock and Hybrid"
    );
}

// =================================================================================================
// Scenario 4: the displaced old value is dropped exactly once, including address reuse
// =================================================================================================
//
// Hypothesis H4 (falsified if this regresses): "once the writer bumps the ref counts of all
// debts, someone will eventually drop the old Arc". That is necessary but not sufficient; the
// exact-once property also has to survive (a) guards that are paid back by the writer and later
// dropped by the reader, (b) guards turned into owned Arcs via into_inner, and (c) freed memory
// being reused for a new allocation at the same address (ABA by address reuse). The fast path
// builds its guard from the *second* (`confirm`) pointer load for exactly reason (c); these
// checks pin that down.

#[test]
fn litmus_old_value_drops_exactly_once_under_mixed_guards() {
    for strategy_hint in ["default", "no-fast"] {
        let drops = Arc::new(AtomicUsize::new(0));
        let by_tag = histogram();
        let shared: Swap<DefaultStrategy> = Swap::from(make(1, &drops, &by_tag));

        // Mix protections: some fast-backed guards, more than eight to force fallback-backed ones,
        // one promoted to a full Arc via into_inner, and one load_full.
        let mut debt_guards: Vec<_> = (0..10).map(|_| shared.load()).collect();
        let promoted = Guard::into_inner(debt_guards.pop().unwrap());
        let owned = shared.load_full();
        assert_eq!(promoted.tag, 1);
        assert_eq!(owned.tag, 1);

        // Two consecutive replacements while the mixed protections are alive.
        shared.store(make(2, &drops, &by_tag));
        shared.store(make(3, &drops, &by_tag));
        assert_eq!(
            drops_of(&by_tag, 1),
            0,
            "tag 1 dropped while {strategy_hint} protections still live"
        );

        // Releasing debt-backed guards after the writer already paid them must not decrement tag
        // 1 again; they now behave as owned Arcs whose writer-paid balance cancels on drop.
        drop(debt_guards);
        assert_eq!(
            drops_of(&by_tag, 1),
            0,
            "tag 1 dropped early on guard release"
        );
        drop(promoted);
        drop(owned);

        // Tag 1 still referenced by the swap's earlier snapshot chain? No: stores left tag 3 in
        // storage, so once every protection is gone tag 1 drops exactly once.
        assert_eq!(
            drops_of(&by_tag, 1),
            1,
            "tag 1 must drop exactly once (strategy hint {strategy_hint})"
        );
        assert_eq!(drops_of(&by_tag, 2), 1, "tag 2 must drop exactly once");

        drop(shared);
        assert_eq!(drops_of(&by_tag, 3), 1, "tag 3 must drop exactly once");
        assert_eq!(drops.load(Relaxed), 3);
    }
}

#[test]
fn litmus_provenance_survives_address_reuse() {
    // ABA by address reuse. A fast-slot load reads pointer P, then the writer replaces it with Q
    // (so P may free), the memory at P is reallocated for a different object, and the slot would
    // wrongly look "unchanged" if the reader compared only the first read. The reader must build
    // its guard from the *second* (confirm) read with current provenance. We cannot force jemalloc
    // to reuse an address, so the loop searches for the real (and common) reuse event and, when it
    // occurs, validates the invariant on the exact schedule; tag identities make every outcome
    // diagnostic. No sleep is involved: each iteration makes forward progress.
    let _guard = seam_lock();
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();

    let mut saw_reuse = false;
    for round in 0..200u64 {
        // Disjoint tag spaces per round: payloads at 2*round+{1,2}, recycled allocations at a
        // sparse high range, so the per-tag ledger can attribute every drop unambiguously.
        let tag_a = 2 * round + 1;
        let tag_b = 2 * round + 2;
        // Begin with exactly one strong reference: the one inside the swap.
        let swap: Swap<DefaultStrategy> = Swap::from(make(tag_a, &drops, &by_tag));

        // Snapshot the address of tag_a through a debt guard (no owned ref), then release it so
        // the only surviving reference is the storage's.
        let addr_a = {
            let probe = swap.load();
            assert_eq!(probe.tag, tag_a);
            Arc::as_ptr(&*probe) as usize
        };

        // Replace with tag_b and drop the storage's reference to tag_a by emptying the swap, so
        // tag_a's allocation is actually freed and eligible for immediate reuse.
        let b = make(tag_b, &drops, &by_tag);
        swap.store(b);
        let addr_b = {
            let g = swap.load();
            Arc::as_ptr(&*g) as usize
        };

        // New allocation of the same size class. If the allocator hands back tag_a's address, we
        // have the ABA configuration: address equals addr_a, but the object/type-era is tag_c.
        let tag_c = 1_000_003 + 3 * round;
        let c = make(tag_c, &drops, &by_tag);
        let addr_c = Arc::as_ptr(&c) as usize;
        if addr_c == addr_a {
            saw_reuse = true;
            // The current storage value is still tag_b. A guard acquired *now* (its two pointer
            // reads may straddle nothing here, but the guard's provenance must be tag_b's) must
            // dereference to tag_b even though addr_a is live again with unrelated content behind
            // it. Crucially, the guard pointer must never be confused with the recycled address.
            let g = swap.load();
            assert_eq!(g.tag, tag_b);
            assert_eq!(Arc::as_ptr(&*g) as usize, addr_b);
            assert_ne!(Arc::as_ptr(&*g) as usize, addr_c);
            drop(g);
        }
        drop(c);
        drop(swap);
        // Both payloads of the round must be gone exactly once by now.
        assert_eq!(
            drops_of(&by_tag, tag_a),
            1,
            "round {round}: tag {tag_a} drops"
        );
        assert_eq!(
            drops_of(&by_tag, tag_b),
            1,
            "round {round}: tag {tag_b} drops"
        );
    }
    assert!(
        saw_reuse,
        "allocator never reused an address; the provenance schedule was not exercised"
    );
}

// =================================================================================================
// Scenario 5: linearisation points of guard drop and Guard::into_inner
// =================================================================================================
//
// Hypothesis H5 (falsified if this regresses): "dropping a debt guard always decrements the ref
// count". It does not when the writer already paid the debt: the guard then holds an effectively
// owned Arc (the slot's CaS fails), and dropping it must balance *that* owned reference instead.
// `into_inner` must arrange the converse exactly once: add one ref count, try to clear the debt,
// and if the debt was already paid, remove the just-added count. This scenario checks both
// directions with the writer parked relative to each operation, and cross-checks RwLock, where
// every guard is unconditionally owned.

#[test]
fn litmus_guard_drop_after_writer_pays_balances_once() {
    let _guard = seam_lock();
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared: Swap<DefaultStrategy> = Swap::from(make(1, &drops, &by_tag));

    // A fast-slot guard and a forced-fallback guard, both alive across a store. The store's
    // wait_for_readers pays whatever debts it observes; after it returns the guards are owned Arcs
    // in disguise, so dropping them later drops tag 1, but each exactly once.
    let g_fast = shared.load();
    let g_slow = {
        let fillers: Vec<Swap<DefaultStrategy>> = (0..8)
            .map(|i| Swap::from(make(100 + i, &drops, &by_tag)))
            .collect();
        let filler_guards: Vec<_> = fillers.iter().map(|f| f.load()).collect();
        let g = shared.load();
        assert_eq!(g.tag, 1);
        // Keep fillers alive only until the fallback load completes; they may release now without
        // affecting g_slow (which is already confirmed).
        drop(filler_guards);
        drop(fillers);
        g
    };

    shared.store(make(2, &drops, &by_tag));
    assert_eq!(
        drops_of(&by_tag, 1),
        0,
        "tag 1 paid-for but still protected"
    );

    drop(g_fast);
    let after_first = drops_of(&by_tag, 1);
    assert!(
        after_first <= 1,
        "tag 1 dropped twice after first guard drop"
    );
    drop(g_slow);
    assert_eq!(drops_of(&by_tag, 1), 1, "tag 1 must drop exactly once");

    // into_inner on a *fresh* guard must leave exactly one balanced Arc behind: promote, drop the
    // guard source (moved), then the promoted Arc is the sole holder besides storage if equal.
    let promoted = Guard::into_inner(shared.load());
    assert_eq!(promoted.tag, 2);
    drop(promoted);
    assert_eq!(drops_of(&by_tag, 2), 0, "tag 2 still in storage");

    drop(shared);
    assert_eq!(drops_of(&by_tag, 2), 1, "tag 2 must drop exactly once");
    assert_eq!(drops.load(Relaxed), 10, "8 fillers + tags 1 and 2");
}

#[test]
fn litmus_into_inner_and_drop_rwlock_crosscheck() {
    // On RwLock every guard owns a reference from the start. The externally visible drop ledger
    // for "promote a guard, release guards, destroy the swap" must match the hybrid ledger.
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared: Swap<RwLock<()>> = Swap::from(make(1, &drops, &by_tag));
    let g1 = shared.load();
    let g2 = shared.load();
    let promoted = Guard::into_inner(shared.load());
    shared.store(make(2, &drops, &by_tag));
    drop(g1);
    drop(g2);
    assert_eq!(drops_of(&by_tag, 1), 0, "promoted Arc keeps tag 1 alive");
    drop(promoted);
    assert_eq!(
        drops_of(&by_tag, 1),
        1,
        "tag 1 dropped once after promotion release"
    );
    drop(shared);
    assert_eq!(drops_of(&by_tag, 2), 1);
    assert_eq!(drops.load(Relaxed), 2);
}

#[test]
fn litmus_unprotected_reader_cannot_outlive_wait_for_readers() {
    // Allocator-independent falsifier for H2's AfterLoad window.
    //
    //   reader: reserve GEN, load candidate P              [park BeforeConfirm-equivalent window]
    //   writer: swap P -> Q, run wait_for_readers, drop own P
    //
    // If the reader's not-yet-published protection were missed by the writer, P's count reaches
    // zero inside the writer window and P is dropped *before the reader resumes*. The correct
    // algorithm guarantees the opposite: because the reservation (GEN + active address) precedes
    // the candidate load under SeqCst, the writer's traversal either pays the slot debt or hands
    // over a protected replacement, so P is alive (already drop-count 0) when the reader wakes.
    //
    // The decisive observable is the drop count while the reader is still parked, which does not
    // depend on allocator address reuse and therefore fails deterministically on a broken writer.
    let _guard = seam_lock();
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared: Swap<DefaultStrategy> = Swap::from(make(1, &drops, &by_tag));
    let storage_addr = test_ctl::storage_addr_of(&shared);

    test_ctl::arm(storage_addr, Window::BeforeConfirm);
    let shared_ref: &Swap<DefaultStrategy> = &shared;
    thread::scope(|scope| {
        let d2 = drops.clone();
        let b2 = by_tag.clone();
        let reader = scope.spawn(move |_| {
            let fillers: Vec<Swap<DefaultStrategy>> = (0..8)
                .map(|i| Swap::from(make(100 + i, &d2, &b2)))
                .collect();
            let fg: Vec<_> = fillers.iter().map(|f| f.load()).collect();
            let g = shared_ref.load();
            let tag = g.tag;
            let owned = Guard::into_inner(g);
            (tag, owned, fillers, fg)
        });

        test_ctl::wait_entered();
        // The reader has published debt(P) and is parked before clearing GEN. Swap and finish the
        // whole writer protocol while it sits there.
        let p = shared.swap(make(2, &drops, &by_tag));
        assert_eq!(p.tag, 1);
        let drops_while_parked = drops_of(&by_tag, 1);
        // Drop the writer-owned P as well: with no outstanding protection P would die right here.
        drop(p);
        let drops_after_p_release = drops_of(&by_tag, 1);
        assert_eq!(
            drops_after_p_release, 0,
            "P freed in the writer window while a reader still needs it (drops observed: \
             parked={drops_while_parked}, after_release={drops_after_p_release})"
        );
        test_ctl::release();

        let (tag, owned, fillers, fg) = reader.join().unwrap();
        // The reader resumes onto a protected object: either P (debt paid by writer) or Q
        // (handover). Either way it dereferences valid memory here.
        assert!(
            tag == 1 || tag == 2,
            "reader resumed onto invalid tag {}",
            tag
        );
        assert_eq!(owned.tag, tag, "promoted Arc lost the protected identity");
        drop(owned);
        drop(fg);
        drop(fillers);
    })
    .unwrap();

    drop(shared);
    // P (tag 1) and Q (tag 2) each drop exactly once once all protections are gone.
    assert_eq!(drops_of(&by_tag, 1), 1, "P not dropped exactly once");
    assert_eq!(drops_of(&by_tag, 2), 1, "Q not dropped exactly once");
    for i in 0..8u64 {
        assert_eq!(drops_of(&by_tag, 100 + i), 1, "filler {i} not dropped once");
    }
    assert_eq!(drops.load(Relaxed), 10);
}

// =================================================================================================
// Scenario 6: memory-ordering regression for the fast-path double read (the #198/#204 class)
// =================================================================================================
//
// Most dangerous counterexample in this audit (see CHANGELOG.md, "Most dangerous counterexample"):
// a fast acquisition is *two* SeqCst storage loads with a SeqCst debt publication in between,
// and the guard is built from the *second* load. If the first load were reused, or the
// confirmation weakened, a writer could free the old object while the reader dereferences it.
// x86-TSO hides the ordering half, so the deterministic scenario here pins what a test can
// observe: a fast guard obtained immediately before a store keeps that exact old object alive
// (drop count 0) across arbitrarily many writers, and the object drops exactly once after all
// guards go. The orderings are additionally guarded by a compile-time unit test in
// `src/strategy/hybrid.rs` (`fast_confirm_ordering_is_seqcst`).

#[test]
fn litmus_fast_guard_outlives_concurrent_writers() {
    let drops = Arc::new(AtomicUsize::new(0));
    let by_tag = histogram();
    let shared: Swap<DefaultStrategy> = Swap::from(make(1, &drops, &by_tag));

    // Hold up to eight (all fast slots) guards on tag 1, then rotate the storage many times.
    // Every fast guard keeps its own protection; tag 1 must not drop while any of them lives.
    let guards: Vec<_> = (0..8).map(|_| shared.load()).collect();
    for i in 0..50u64 {
        shared.store(make(2 + i, &drops, &by_tag));
    }
    for g in &guards {
        assert_eq!(g.tag, 1, "fast guard observed a value it never loaded");
    }
    assert_eq!(
        drops_of(&by_tag, 1),
        0,
        "tag 1 freed behind live fast guards"
    );

    // Release guards one at a time: the writer already paid these debts, so none of the drops
    // may hit tag 1 until the last writer-paid balance is reconciled. The exact-once invariant is
    // checked at the end, not per-drop, because paid debts convert guards into owned Arcs.
    drop(guards);
    drop(shared);
    assert_eq!(drops_of(&by_tag, 1), 1, "tag 1 must drop exactly once");
    for i in 0..50u64 {
        assert_eq!(
            drops_of(&by_tag, 2 + i),
            1,
            "rotated payload {} dropped once",
            i
        );
    }
}
