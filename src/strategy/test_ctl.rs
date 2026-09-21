//! Deterministic-interleaving control surface for the internal litmus suite.
//!
//! This module exists solely under the `internal-test-strategies` feature and provides the tests
//! a way to pause a *reader* at two precisely defined points inside the helping fallback
//! ([`crate::strategy::hybrid::HybridProtection`]):
//!
//! * [`Window::AfterReserve`] – the reader has published the storage address and a generation in
//!   its helping control (`Slots::get_debt`) but has not loaded the pointer yet. A writer that
//!   replaces the storage during this pause is forced through the collision/handover path of
//!   [`crate::debt::Debt::pay_all`] / `Slots::help`.
//! * [`Window::AfterLoad`] – the reader has loaded the candidate pointer but has not yet
//!   confirmed it into the debt slot (`Slots::confirm`). A writer swapping in this window must
//!   either pay the in-flight debt or have its `wait_for_readers` traversal miss the reader; the
//!   old pointer must not be freed either way.
//!
//! # Why a "one-shot reader pause" instead of random sleeps
//!
//! Timing-based tests cannot falsify anything: they fail on loaded CI machines and pass on the
//! buggy implementation whenever the schedule happens not to hit the collision. The pause here is
//! an explicit two-thread rendezvous driven entirely by atomics, so the interleaving is fixed:
//!
//! 1. The test thread (playing the writer role) arms the seam with [`arm`] for the exact storage
//!    address and window.
//! 2. The reader thread enters the fallback, performs the prefix of the load protocol and calls
//!    [`maybe_pause`]. Exactly one such call transitions `ARMED` → `ENTERED` and blocks.
//! 3. The writer waits for `ENTERED` (so the reader really is inside the window), runs the
//!    competing write, then calls [`release`].
//! 4. The reader observes `RELEASED`, completes the protocol and returns.
//!
//! Only a *reader* ever parks. The writer itself performs loads while paying debts (it needs to
//! synthesise a replacement for a colliding reader); those calls observe `ENTERED`/`RELEASED` and
//! therefore return immediately, which prevents self-deadlock even when fast slots are disabled.
//!
//! Nothing here is random, nothing touches the network or the file system and there are no
//! timeouts. If the rendezvous breaks, [`maybe_pause`] spins on an atomic that is always set by
//! the test harness; the litmus suite additionally serialises all tests that use the seam.

use core::sync::atomic::{AtomicUsize, Ordering::*};

/// Points inside the reader's fallback protocol at which a pause can be requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Window {
    /// After publishing the generation/active address, before loading the pointer.
    AfterReserve,
    /// After loading the candidate pointer, before confirming it into the debt slot.
    AfterLoad,
    /// Inside confirmation: the candidate is already published in the debt slot but the control
    /// still carries the generation. A writer in this window sees the slot debt and pays it.
    BeforeConfirm,
}

// Seam state. The address fields doubles as the arming key: a pause fires only for the exact
// storage (the `AtomicPtr` inside the `ArcSwapAny`) the test armed, so other instances keep
// working normally inside the same process.
const DISARMED: usize = 0;
const ARMED: usize = 1;
const ENTERED: usize = 2;
const RELEASED: usize = 3;

static STATE: AtomicUsize = AtomicUsize::new(DISARMED);
static STORAGE_ADDR: AtomicUsize = AtomicUsize::new(0);
static WINDOW: AtomicUsize = AtomicUsize::new(0);

impl Window {
    const fn code(self) -> usize {
        match self {
            Window::AfterReserve => 1,
            Window::AfterLoad => 2,
            Window::BeforeConfirm => 3,
        }
    }
}

/// Arm the seam for one reader entering `window` on the storage at `storage_addr`.
///
/// Must be called before the reader starts the load. The seam stays armed until consumed by that
/// reader and disarmed again with [`release`].
pub fn arm(storage_addr: usize, window: Window) {
    let prev = STATE.swap(DISARMED, AcqRel);
    debug_assert_eq!(
        prev, DISARMED,
        "litmus seam armed while a previous rendezvous was still active",
    );
    STORAGE_ADDR.store(storage_addr, SeqCst);
    WINDOW.store(window.code(), SeqCst);
    // Publish the key before the state: paired with the reader's SeqCst load in maybe_pause.
    STATE.store(ARMED, SeqCst);
}

/// Wait until the reader has entered the armed window.
///
/// Returning from here means the reader has completed every step before the window and is parked,
/// so the racing write happens at the exact linearisation point the test targets.
pub fn wait_entered() {
    while STATE.load(SeqCst) != ENTERED {
        core::hint::spin_loop();
    }
}

/// Release the parked reader and wait until it disarms the seam.
pub fn release() {
    let prev = STATE.swap(RELEASED, SeqCst);
    debug_assert!(
        prev == ENTERED,
        "litmus seam released without a parked reader (state {})",
        prev
    );
    while STATE.load(SeqCst) != DISARMED {
        core::hint::spin_loop();
    }
}

/// Called inside the reader fallback. Parks exactly one matching reader for the armed window.
///
/// Writers call into the fallback too (to build helping replacements); they never block because
/// by the time they run, the state is already `ENTERED` or `RELEASED`, not `ARMED`.
#[inline]
pub fn maybe_pause(storage_addr: usize, window: Window) {
    if STATE.load(SeqCst) != ARMED {
        return;
    }
    if STORAGE_ADDR.load(SeqCst) != storage_addr || WINDOW.load(SeqCst) != window.code() {
        return;
    }
    // We are the first matching call since arming: claim the rendezvous. Another matching reader
    // could in principle race us here; the litmus suite creates only one reader, and a writer's
    // recursive load sees ENTERED afterwards. Using compare_exchange keeps even that honest.
    if STATE
        .compare_exchange(ARMED, ENTERED, SeqCst, SeqCst)
        .is_err()
    {
        return;
    }
    while STATE.load(SeqCst) != RELEASED {
        core::hint::spin_loop();
    }
    STATE.store(DISARMED, SeqCst);
}

use crate::strategy::Strategy;
use crate::{ArcSwapAny, RefCnt};

/// Returns the address of the internal pointer storage of `swap`.
///
/// The litmus suite arms the reader seam per storage, because the writer must only pause readers
/// of the `ArcSwapAny` under test, never unrelated instances sharing the same thread node.
pub fn storage_addr_of<T, S>(swap: &ArcSwapAny<T, S>) -> usize
where
    T: RefCnt,
    S: Strategy<T>,
{
    swap.ptr.as_ptr() as usize
}

/// Diagnostics: how often a writer observed a helping generation for the armed storage.
static HELPER_GEN_SEEN: AtomicUsize = AtomicUsize::new(0);
/// Diagnostics: how often the handover CAS succeeded.
static HELPER_HANDOVER_OK: AtomicUsize = AtomicUsize::new(0);

pub fn helper_saw_gen() {
    HELPER_GEN_SEEN.fetch_add(1, Relaxed);
}
pub fn helper_handover_ok() {
    HELPER_HANDOVER_OK.fetch_add(1, Relaxed);
}
pub fn helper_counters() -> (usize, usize) {
    (
        HELPER_GEN_SEEN.load(Relaxed),
        HELPER_HANDOVER_OK.load(Relaxed),
    )
}
