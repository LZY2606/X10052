//! Deterministic-interleaving probes for the debt/helping audit tests.
//!
//! This module exists only under the `internal-test-strategies` feature and is used exclusively by
//! `tests/litmus.rs`. It provides two things:
//!
//! * A fixed set of counters that record *which* provenance branch a `load`/`cas`/`drop` actually
//!   took (fast slot, fast slot whose debt was already paid by a writer, helping fallback confirmed
//!   or upgraded, writer-side pay/help events). Counters make otherwise invisible linearization
//!   choices observable without weakening any memory ordering.
//! * One-shot "checkpoints": named rendez-vous points inside the reader/writer critical sections.
//!   A test arms a checkpoint, lets a thread run until the probe fires and then explicitly releases
//!   it, placing the *other* thread at an exact instruction in the interleaving. This replaces
//!   sleeps/timing assumptions with deterministic synchronization.
//!
//! Every wait has a bounded deadline and panics with the checkpoint name and the observed counter
//! snapshot, so a stuck or mis-wired interleaving fails with diagnostic context instead of hanging
//! the test process.

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::atomic;
use std::time::{Duration, Instant};

// Counter indices. Keep the names/order in sync with `tests/litmus.rs`.
/// Fast slot confirmed: both storage reads matched and the debt is live.
pub const FAST_CONFIRMED: usize = 0;
/// Fast confirm lost the race; the slot had already been paid by a writer.
pub const FAST_ALREADY_PAID: usize = 1;
/// Helping fallback confirmed the reader's own candidate debt.
pub const FALLBACK_CONFIRMED: usize = 2;
/// Helping fallback was handed a writer-protected replacement.
pub const FALLBACK_UPGRADED: usize = 3;
/// A writer paid one debt slot during pay_all.
pub const PAY_SLOT: usize = 4;
/// A writer completed a reader's half-published helping reservation.
pub const HELP_SUCCESS: usize = 5;
/// Guard drop returned a live debt to NONE.
pub const GUARD_DROP_DEBT_PAID: usize = 6;
/// Guard drop found its debt pre-paid and destroyed the owned Arc.
pub const GUARD_DROP_ALREADY_PAID: usize = 7;
/// into_inner cancelled a live debt with the speculative increment.
pub const INTO_INNER_DEBT_PAID: usize = 8;
/// into_inner found the debt pre-paid and rolled the increment back.
pub const INTO_INNER_ALREADY_PAID: usize = 9;
pub(crate) const COUNTERS: usize = 10;

const COUNTER_NAMES: [&str; COUNTERS] = [
    "FAST_CONFIRMED",
    "FAST_ALREADY_PAID",
    "FALLBACK_CONFIRMED",
    "FALLBACK_UPGRADED",
    "PAY_SLOT",
    "HELP_SUCCESS",
    "GUARD_DROP_DEBT_PAID",
    "GUARD_DROP_ALREADY_PAID",
    "INTO_INNER_DEBT_PAID",
    "INTO_INNER_ALREADY_PAID",
];

// Checkpoint indices.
/// Reader parked right after claiming a fast slot, before the confirming load.
pub const CP_FAST_CLAIMED: usize = 0;
/// Reader parked immediately before the second (confirming) storage load on the fast path.
pub const CP_FAST_CONFIRM: usize = 1;
/// Reader parked after publishing the helping generation, before loading the candidate.
pub const CP_FALLBACK_GEN: usize = 2;
/// Reader parked after the candidate load, before confirming the helping transaction.
pub const CP_FALLBACK_CANDIDATE: usize = 3;
/// Reader parked at the helping confirm attempt itself.
pub const CP_FALLBACK_CONFIRM: usize = 4;
/// Guard parked at entry to `into_inner`, before the speculative increment and debt pay-back.
pub const CP_INTO_INNER: usize = 5;
pub(crate) const CHECKPOINTS: usize = 6;

pub(crate) const CP_NAMES: [&str; CHECKPOINTS] = [
    "CP_FAST_CLAIMED",
    "CP_FAST_CONFIRM",
    "CP_FALLBACK_GEN",
    "CP_FALLBACK_CANDIDATE",
    "CP_FALLBACK_CONFIRM",
    "CP_INTO_INNER",
];

static COUNTS: [AtomicU64; COUNTERS] =
    [const { AtomicU64::new(0) }; COUNTERS];

// One armed flag and one fired flag per checkpoint.
static ARMED: [AtomicBool; CHECKPOINTS] =
    [const { AtomicBool::new(false) }; CHECKPOINTS];
static FIRED: [AtomicBool; CHECKPOINTS] =
    [const { AtomicBool::new(false) }; CHECKPOINTS];

const WAIT_LIMIT: Duration = Duration::from_secs(15);

#[inline]
pub(crate) fn count(idx: usize) {
    COUNTS[idx].fetch_add(1, Ordering::Relaxed);
}

/// Called from the production-shaped code at a named interleaving point.
///
/// If the checkpoint is armed, the calling thread records that it has reached the point and parks
/// until [`release`] is called by the test driver. A release of an unarmed checkpoint is a no-op
/// (the production path never pays any synchronization cost beyond a relaxed load).
pub(crate) fn checkpoint(idx: usize) {
    if ARMED[idx].load(Ordering::Relaxed) {
        // One-shot rendez-vous: only the first arrival parks. Later arrivals pass through.
        //
        // This matters because a writer pays debts with a nested full `load().into_inner()`,
        // which executes the very same fast/helping probes. The test driver arms a checkpoint
        // for the reader and only starts the writer after the reader fired, so the writer's
        // nested arrival always loses this CAS and must proceed instead of parking on its own
        // rendez-vous.
        if FIRED[idx]
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let deadline = Instant::now() + WAIT_LIMIT;
            while ARMED[idx].load(Ordering::SeqCst) {
                if Instant::now() >= deadline {
                    panic!(
                        "litmus checkpoint {} stuck while armed; counters = {}",
                        CP_NAMES[idx],
                        snapshot()
                    );
                }
                std::thread::park_timeout(Duration::from_millis(1));
            }
            atomic::fence(Ordering::SeqCst);
        }
    }
}

/// Reset every counter and checkpoint. Litmus tests are serialized by the driver and each calls
/// this first, so leftovers from a previous scenario can never contaminate assertions.
/// Reset every counter and checkpoint before a scenario.
pub fn reset() {
    for c in COUNTS.iter() {
        c.store(0, Ordering::SeqCst);
    }
    for a in ARMED.iter() {
        a.store(false, Ordering::SeqCst);
    }
    for f in FIRED.iter() {
        f.store(false, Ordering::SeqCst);
    }
}

/// Arm a checkpoint before letting the probed thread run.
/// Arm a checkpoint before letting the probed thread run.
pub fn arm(idx: usize) {
    FIRED[idx].store(false, Ordering::SeqCst);
    ARMED[idx].store(true, Ordering::SeqCst);
}

/// Release a thread parked at a checkpoint.
/// Release a thread parked at a checkpoint.
pub fn release(idx: usize) {
    ARMED[idx].store(false, Ordering::SeqCst);
}

/// Block until the probed thread reaches the checkpoint.
/// Block until the probed thread reaches the checkpoint.
pub fn wait_fired(idx: usize) {
    let deadline = Instant::now() + WAIT_LIMIT;
    while !FIRED[idx].load(Ordering::SeqCst) {
        if Instant::now() >= deadline {
            panic!(
                "litmus timed out waiting for {}; counters = {}",
                CP_NAMES[idx],
                snapshot()
            );
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

/// Read one counter.
/// Read one counter.
pub fn counter(idx: usize) -> u64 {
    COUNTS[idx].load(Ordering::SeqCst)
}

fn snapshot() -> String {
    COUNTER_NAMES
        .iter()
        .zip(COUNTS.iter())
        .map(|(name, c)| format!("{name}={}", c.load(Ordering::SeqCst)))
        .collect::<Vec<_>>()
        .join(",")
}
