//! Test-only interleave hooks for the internal strategies.
//!
//! This module exists only under the `internal-test-strategies` feature and comes with *no*
//! stability guarantees. It is the single place where the otherwise lock-free algorithms of the
//! hybrid strategy expose their internal linearization steps to tests.
//!
//! # Why hooks are needed
//!
//! The correctness arguments in `ANALYSIS.md` talk about specific linearization points:
//!
//! * the second (confirming) `SeqCst` load of a fast debt,
//! * the generation publication and the confirmation `swap` of the helping/fallback slot,
//! * the `SeqCst` `compare_exchange` inside a CAS and the subsequent debt walk,
//! * the writer's helping pass vs. its debt-slot scan inside `pay_all`.
//!
//! A randomized stress test can hit these points, but can't pin a *specific* interleaving. The
//! litmus suite in `tests/litmus.rs` registers a hook here, parks the thread that reaches an armed
//! point and drives the other thread through the exact dangerous window. No sleeps, no timing
//! assumptions are involved: every step of the schedule is explicitly released by the test.
//!
//! The default strategy (and every `HybridStrategy`, regardless of its `Config`) calls [`fire`]
//! at the audited points when this feature is compiled in. When no hook is registered the call is
//! a single relaxed atomic load of a null pointer and inlines to nothing.

use core::mem;
use core::ptr;
use core::sync::atomic::{AtomicPtr, Ordering};

/// The audited linearization steps of the hybrid strategy.
///
/// The same point is reported on every thread; hook implementations filter by thread and/or by
/// the address of the `AtomicPtr` storage passed to the callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum HookPoint {
    /// Fast path: the debt slot already contains the pointer, the confirming second
    /// `storage.load(SeqCst)` has not happened yet.
    ///
    /// A reader parked here has *published* a debt but nobody can tell yet which pointer the
    /// linearization will return.
    FastPreConfirm,

    /// Fallback/helping path: the generation is published in the control (and `active_addr` set),
    /// but the candidate pointer has not been loaded yet.
    ///
    /// A writer that walks debts while a reader is parked here takes the collision path in
    /// [`help`](crate::debt::Debt::pay_all) and can offer a replacement.
    FallbackReserved,

    /// Fallback/helping path: the candidate has been loaded but the generation has not been
    /// confirmed back to `IDLE` yet. The writer may still observe the generation and help.
    FallbackLoaded,

    /// CAS: the loaded value was observed equal to `current`, but the installing
    /// `compare_exchange_weak(SeqCst, _)` has not run yet.
    ///
    /// This is precisely the gap between the observation point and the installation point of a
    /// CAS; another writer winning here forces the CAS to retry.
    CasObservedEqual,

    /// CAS: the `compare_exchange_weak` succeeded, but `wait_for_readers` has not run yet. The old
    /// pointer is no longer in storage and may still be kept alive only by outstanding debts.
    CasWon,

    /// Writer: `pay_all` is about to traverse the thread-node chain for the just-removed pointer.
    WriterPayAllStart,

    /// Writer: about to perform the helping pass on one node (the generation collision check).
    WriterNodeBeforeHelp,

    /// Writer: the helping pass for one node is done, but the fast/helping debt slots of that
    /// node have not been scanned and paid yet.
    WriterNodeAfterHelp,

    /// Writer: all debt slots of one node have been scanned.
    WriterNodeSlotsDone,
}

/// The signature of an interleave callback.
///
/// It receives the hook point and the address of the `AtomicPtr` storage the operation belongs
/// to, so a single callback can distinguish several `ArcSwap` instances.
pub type HookFn = fn(HookPoint, usize);

static HOOK: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());

/// Installs (or clears, with `None`) the global interleave callback.
///
/// Tests call this before spawning the orchestrated threads and clear it on teardown. The store
/// is only an coordination mechanism of the test harness; litmus tests synchronize the
/// registration with thread barriers.
#[doc(hidden)]
pub fn set_hook(hook: Option<HookFn>) {
    let ptr = match hook {
        Some(hook) => hook as *mut (),
        None => ptr::null_mut(),
    };
    HOOK.store(ptr, Ordering::Relaxed);
}

/// Fires a hook point.
///
/// This is the only call site of the registered callback. The relaxed load is enough: the
/// registration happens-before the orchestrated threads start (test barriers provide the actual
/// synchronization) and the callback itself does whatever synchronization it needs.
#[inline]
pub(crate) fn fire(point: HookPoint, storage_addr: usize) {
    let hook = HOOK.load(Ordering::Relaxed);
    if !hook.is_null() {
        // SAFETY: only `set_hook` writes the pointer, with a real `HookFn` or null, and tests
        // keep the hook installed for the whole lifetime of the orchestrated threads.
        let hook: HookFn = unsafe { mem::transmute::<*mut (), HookFn>(hook) };
        hook(point, storage_addr);
    }
}
