# ANALYSIS — Debt Helping, Guard Fallback and CAS Linearisation Audit

Scope: `arc-swap` hybrid strategy, along the call chains `load`, `compare_and_swap`,
`wait_for_readers`, guard `Drop`, and `Guard::into_inner`. This document records where pointer
provenance enters the system, where each protection becomes linearised, how the writer pays
reader debts, and how the new `tests/litmus.rs` suite falsifies the design assumptions that used
to be argued only on paper.

The audited tree is upstream `vorner/arc-swap@147d6c0` (the 1.9.x line that already promoted the
orderings from the #198/#200/#204 fixes). No parallel implementation was introduced; the only
production-touched file is `src/strategy/hybrid.rs`, and only by adding two feature-gated
test seams (compiled out unless `internal-test-strategies` is on) plus a third seam in
`src/debt/helping.rs`. Everything else is tests and documentation.

## 1. Entry points and the objects in play

* Public entry: `ArcSwapAny::{load, swap, store, compare_and_swap, rcu, into_inner}` in
  `src/lib.rs`. `store` is `drop(swap(_))`; `rcu` is a retry loop over `compare_and_swap`.
* Storage state: a single `AtomicPtr<T::Base>` field `ptr` on `ArcSwapAny` (`src/lib.rs`), plus
  the strategy value (`HybridStrategy<Cfg>`, which currently holds only a `Cfg` marker).
* Reference accounting abstraction: `RefCnt` in `src/ref_cnt.rs`. Every path converts an `Arc`
  to/from a raw pointer with `into_ptr`/`from_ptr`/`as_ptr` **without** touching the strong
  count; `inc`/`dec` are the only operations that move the count.
* Protection objects:
  * `HybridProtection<T>` (`src/strategy/hybrid.rs`) — `{ debt: Option<&'static Debt>, ptr:
    ManuallyDrop<T> }`. `debt = Some(_)` means "the `ptr` is owed to a slot, no owned ref";
    `debt = None` means "the `ptr` is a normal owned `T`".
  * `Guard<T, S>` in `src/lib.rs` is a thin newtype around `S::Protected`.
* Debt storage:
  * eight *fast* slots per thread node (`src/debt/fast.rs`, `DEBT_SLOT_CNT = 8`),
  * one *helping* slot per thread node plus the generation/handover protocol
    (`src/debt/helping.rs`),
  * a prepend-only linked list of per-thread `Node`s walked by writers (`src/debt/list.rs`).
* Cache layer: `Cache` in `src/cache.rs` holds a private owned `T`; its `revalidate` compares
  the storage pointer with `Relaxed` and, on mismatch, replaces the cached value with a
  `load_full`. It deliberately delays reclamation (it owns a full reference) and is *not* part
  of the debt protocol, so it does not change any linearisation point — it is an ordinary client
  of `load`.
* Weak: `ArcSwapWeak` (`src/weak.rs`) is the same protocol over `Weak`; the audit's pointer
  reasoning is identical because `Weak` satisfies the same `RefCnt` contract.

## 2. State machine of one protection

`HybridProtection` is produced by exactly one of three outcomes, and its `debt` field records
which:

| Path | Code | `debt` | Meaning / balancing |
| --- | --- | --- | --- |
| Fast success | `attempt` | `Some(slot)` | slot stores the pointer; writer or guard drop pays it |
| Fast collision, debt already paid | `attempt` | `None` | writer paid between the two loads; the pointer is effectively owned |
| Helping confirmed | `fallback` → `Ok` (converted via `from_inner`) | `None` | confirmed debt is immediately paid inside `fallback`, leaving an owned `T` |
| Helping handover | `fallback` → `Err` | `None` | writer supplied an already protected replacement; the reader's unused candidate debt is reconciled |

The conversion of an `Ok` helping confirmation into an owned `T`
(`Self::from_inner(Self::new(candidate, Some(debt)).into_inner())`) is the bridge between the
two protocols: `into_inner` adds one strong count and clears the debt, so the returned
protection is in the same owned state as a `load_full` even though it started in the helping
slot.


## 3. Linearisation points, path by path

All line references are to the audited source.

### 3.1 `load` fast path — `HybridProtection::attempt`

```
let ptr     = storage.load(SeqCst);      // (A) first read (candidate)
let debt    = node.new_fast(ptr as usize)?;   // swap NONE -> ptr in slot, SeqCst
let confirm = storage.load(SeqCst);      // (B) confirming read
```

* The linearisation point is the second read **(B)**, never (A). The guard is built from
  `confirm`. The comment at the `ptr == confirm` branch records why: the addresses can compare
  equal after free-and-reuse while the provenance differs, so using `ptr` would alias a recycled
  allocation.
* If `ptr == confirm`, the debt publication and the two reads bracket no writer: the slot debt is
  the protection, linearised at (B).
* If they differ, the reader tries `debt.pay::<T>(ptr)`:
  * pay succeeds → the writer has not paid this slot; the reader abandons the stale debt and
    retries via fallback;
  * pay fails → the writer already bumped the count, so the same address is now an *owned*
    reference and the reader returns `Self::new(ptr, None)`.
* Ordering: (A), the slot `swap`, and (B) are all `SeqCst`. The total order on these RMW/loads
  with the writer's `swap(SeqCst)` partitions histories into "debt visible before the writer's
  traversal" vs "reader observed the new pointer".

### 3.2 `load` fallback — `HybridProtection::fallback`

```
let gen       = node.new_helping(storage_addr);   // (H1) active_addr + GEN in control, SeqCst
let candidate = storage.load(SeqCst);             // (H2)
confirm_helping(gen, candidate):
  slot.swap(candidate)  then control.swap(IDLE)
```

* (H1) publishes **which storage** is being read (`active_addr`) and a fresh generation, before
  the pointer is known. This is what lets a writer resolve a half-finished reservation without
  waiting: it can synthesise a load itself.
* The linearisation point of a *confirmed* fallback load is the `control.swap(IDLE)` observing
  the same `gen` (`Slots::confirm`); after that the slot debt is installed and is immediately
  paid back inside `fallback`, yielding an owned value.
* **Collision path**: if a writer's traversal sees `GEN_TAG` in `control` and `active_addr`
  matches the storage it modified, it performs a full load itself, protects the result, and
  CASes `gen -> REPLACEMENT_TAG(handover)` (`Slots::help`). The reader then finds the
  replacement in `confirm`, uses that pointer, and reconciles its unused candidate debt
  (`unused_debt.pay`; on failure the writer also paid it, so the reader does one balancing
  `T::dec(candidate)`). The reader's linearisation in this case is the writer's handover CAS —
  the reader atomically adopts the writer's load result.
* The envelope (`Handover`, `#[repr(align(4))]`) exists to make room for the two low tag bits
  regardless of the payload pointer's alignment. Generation wraparound is handled by parking the
  thread node in "cooldown" until no writer can still observe the stale generation (`list.rs`).

### 3.3 Writer — `swap` and `wait_for_readers` / `Debt::pay_all`

* `swap`'s linearisation point is `self.ptr.swap(new, SeqCst)` (`src/lib.rs`). Before it, loads
  may return the old pointer; after it, loads return the new one.
* `wait_for_readers(old, storage)` calls `Debt::pay_all`:
  1. `T::inc(&val)` pre-pays one count for the first debt it will clear;
  2. for every `Node` (reserved as an active writer), it first runs `local.help(node, storage_addr,
     replacement)` — resolving an in-flight GEN reservation via handover, synthesising the
     replacement with `self.load(storage).into_inner()`;
  3. then it scans the eight fast slots **and** the helping slot and `pay::<T>(old)`s each one
     that still names `old`, pre-incrementing again per cleared slot;
  4. the final pre-paid balance is consumed by dropping `val`.
* `Debt::pay` is a `compare_exchange(ptr, NONE, SeqCst, SeqCst)`. Success means "this thread now
  owns the reference that backed the debt"; failure means the reader (or another writer) already
  cleared it, so nothing is added.
* When `pay_all` returns, every debt that named `old` and was visible in the SeqCst order either
  has an owned strong count backing it or has already been reconciled; therefore dropping the
  writer's own `old` cannot free memory a reader still dereferences.

### 3.4 `compare_and_swap`

`HybridStrategy::compare_and_swap` is a loop:

1. `let old = load(storage)` — a full protection (linearised per 3.1/3.2).
2. Pointer compare `old.as_ptr() != current.as_raw()` → return `old`; the operation linearises as
   a failed CAS at the load in step 1.
3. `storage.compare_exchange_weak(current, new, SeqCst, Relaxed)`:
   * success — CAS linearisation point; `T::into_ptr(new)` hands the new count to storage, then
     `wait_for_readers(old, storage)` settles all debts, then `T::dec(old)` drops the protection
     acquired in step 1 (the displaced pointer now has exactly its non-storage owners, i.e. the
     returned guard's eventual balance).
   * failure — retry; the loop reacquires, so a swap that lands between step 1 and step 3 causes
     a *failed* CAS returning the current value, never a lost update.

The important subtlety is that the CAS is **not** linearised solely by `compare_exchange`: the
protected `load` and the trailing `wait_for_readers` are part of the safety protocol, and the
returned guard's reference bookkeeping depends on all three.


### 3.5 Guard `Drop`

```
Some(debt) => if debt.pay(ptr) { return }   // reader clears its own debt: no owned ref to drop
None | pay-failed => ManuallyDrop::drop(ptr) // owned reference: decrement
```

The two states are mutually exclusive and exactly balance the writer's accounting:

* If the writer has not paid the debt, the reader's `pay` succeeds; the reader never owned a
  count and must not decrement.
* If the writer already paid (the CAS failed), the protection has silently converted into an
  owned `Arc`; dropping the guard releases that one count.

### 3.6 `Guard::into_inner` / `HybridProtection::into_inner`

For a debt-backed guard it performs `inc` first, then `debt.pay`:

* pay succeeds → the new count stays in the returned `T`, the debt is cleared; balanced;
* pay fails (writer paid) → `dec` the just-added count; the guard keeps the writer-paid balance
  as its owned `T`; balanced.

The `ptr::read` + `mem::forget(self)` moves `T` out without running the protection destructor a
second time. `ArcSwapAny::into_inner` additionally calls `wait_for_readers` once more via the
strategy before reclaiming the stored pointer, so converting the *container* into its inner `T`
also honours outstanding debts.

### 3.7 Provenance trace (summary)

| Pointer seen | Where it is first validated | What keeps it alive | Final release |
| --- | --- | --- | --- |
| fast `confirm` | second SeqCst load in `attempt` | fast slot debt or writer bump | guard `Drop` `pay`/`dec` |
| helping candidate | SeqCst load (H2), then `confirm` | helping slot debt, paid within `fallback` | owned `T` drop |
| handover replacement | writer's own full load during `help` | count carried with the envelope | reader's owned `T` drop |
| CAS `old` | `load` at loop top | that protection + `wait_for_readers` | `T::dec(old)` then guard drop |
| storage value on container drop | `wait_for_readers` in `Drop`/`into_inner` | debt payments | final `T::dec(ptr)` |

## 4. Memory ordering audit

The relevant orderings are deliberately conservative (the 1.9 series, after #198/#200/#204):

* Writer publication (`swap` and the successful CAS in `compare_and_swap`) is `SeqCst`, and so is
  every fast-slot acquisition (`swap`) and both fast reads. A single total order on these
  operations is what makes "slot published before storage swap" imply "writer traversal sees the
  slot".
* `Debt::pay`'s CAS is `SeqCst` even though a Release CAS would publish the increment: the
  failure path observes that the debt was already cleared and must acquire the writer's
  increment before anything relies on the count (the code comment notes that std's `Arc::Drop`
  only fences *after* it observes itself as last owner).
* The helping control word is driven by `SeqCst` at the reservation (the relation with the
  writer's pointer swap) and by AcqRel CASes inside the transaction, with SeqCst reloads of
  `active_addr` to re-confirm that a generation and an address belong to the same load.

These cannot be validated by running on x86 (TSO makes most weak orders behave strongly); the
ordering requirements are therefore locked both by a structural guard
(`strategy::hybrid::ordering_guard`, in-crate unit tests) and by the behavioural interleavings
in the litmus suite.

## 5. Error propagation and diagnostic context

The debt/cas protocols are `unsafe`-internal and do not return `Result`: failures are encoded as
`Option`/`Result` at the slot level (`get_debt → Option`, `confirm → Result<(), usize>`,
`Debt::pay → bool`) and resolved inline, so there is no `#[must_use]` error value that can
silently escape. Where the code `expect`/`assert`s (writer never observes an illegal control
state, slot initially `NONE`, CAS slot alignment), the message names the invariant.

The new test surface follows the same rule: every assertion in `tests/litmus.rs` carries the
scenario, the pointer/tag identities, and where relevant the per-tag drop histogram, so a failure
reports *which* balance broke instead of a generic mismatch. Examples: the CAS ledger failure
prints `(got, expected)` plus the full per-tag map; the parked-reader failure prints both the
"parked" and "after release" counters. No test uses random sleeps, the network, absolute host
paths, or fixture-name special cases: synchronisation is exclusively barriers/scoped-thread
joins and the atomic rendezvous in `strategy::test_ctl`.


## 6. Deterministic interleaving surface (`src/strategy/test_ctl.rs`)

A timing-free rendezvous, available only with `internal-test-strategies`:

* States: `DISARMED → ARMED → ENTERED → RELEASED → DISARMED` (four `AtomicUsize`s).
* `arm(storage_addr, window)` is called by the orchestrating (writer) thread; `maybe_pause` is
  compiled into the reader's `fallback` at three points:
  * `Window::AfterReserve` — after `new_helping` publishes GEN/active address, before the pointer
    load (forces the writer's handover/collision branch);
  * `Window::AfterLoad` — after loading the candidate, before confirming it;
  * `Window::BeforeConfirm` — inside `Slots::confirm`, after the candidate debt is visible in the
    slot and before `control` leaves GEN.
* Only a reader parks: a writer that recursively calls `load` to synthesise a handover
  replacement observes `ENTERED`/`RELEASED` and never blocks, so the protocol cannot self-deadlock
  even with fast slots disabled (`FillFastSlots`).
* The seam is keyed by storage address, so other `ArcSwapAny` instances (including the per-thread
  filler instances used to exhaust fast slots) keep operating normally. All tests that arm it
  serialise through a global mutex, mirroring `tests/stress.rs`.

The seam is zero-cost in normal builds: the call sites are `#[cfg(feature =
"internal-test-strategies")]` and the module is not compiled at all without it.

## 7. Litmus catalogue and the hypothesis each falsifies

All scenarios live in `tests/litmus.rs`, are individually runnable by name, and record a `Trace`
of deterministic step outcomes that is compared against the `RwLock<()>` reference strategy
(`src/strategy/rw_lock.rs`, gated by `internal-test-strategies`).

| Test | Hypothesis it falsifies | Failure under regression |
| --- | --- | --- |
| `litmus_more_than_eight_guards_default` | H1: every guard has a fast slot | with a smaller/broken slot pool, 9th guard loses protection |
| `litmus_more_than_eight_guards_rwlock_crosscheck` | strategies can diverge on overflow | differing traces fail the cross-check |
| `litmus_more_than_eight_guards_no_fast_slots_crosscheck` | fallback changes observable semantics | `FillFastSlots` trace must match the hybrid trace |
| `litmus_fallback_collision_after_reserve` | H2: reservations are atomic vs writers; handover optional | value/provenance mismatch if collision handling is broken |
| `litmus_after_reserve_deterministically_enters_handover` | "the collision is never actually exercised" | counter `handover_ok == 0` fails if the seam stops reaching the branch |
| `litmus_fallback_collision_after_load` | unconfirmed candidate can be freed by a racing writer | tag/provenance/drop assertions fail |
| `litmus_fallback_collision_rwlock_oracle` | collision outcomes must equal some sequential RwLock history | tag/provenance oracle fails |
| `litmus_cas_loses_to_swap_default` | H3: CAS is linearised only by `compare_exchange`; losing CAS may return stale value or leak `new` | returns tag 1 instead of 2, or drop ledger unbalanced |
| `litmus_cas_succeeds_default` | successful CAS need not settle its own reader debt | tag/ledger failure (the writer's own-node debt) |
| `litmus_cas_contention_rwlock_crosscheck` | lock-free CAS history differs from a locked one | trace equality fails for both win/lose schedules |
| `litmus_old_value_drops_exactly_once_under_mixed_guards` | H4: writer bumping counts is enough (ignores exact-once, `into_inner` interactions) | double/missing drop of any tag |
| `litmus_provenance_survives_address_reuse` | pointer *address* equality implies same object | guard aliasing/reused-era assertions; per-tag double-drop |
| `litmus_unprotected_reader_cannot_outlive_wait_for_readers` | `wait_for_readers` may return while a parked reader still needs the old pointer | tag 1 drop count becomes 1 inside the writer window (verified: removing the helping slot from `pay_all` makes the schedule hang/fail deterministically) |
| `litmus_guard_drop_after_writer_pays_balances_once` | H5: dropping a guard always decrements / `into_inner` adds a net count | tag dropped twice or leaked |
| `litmus_into_inner_and_drop_rwlock_crosscheck` | promotion bookkeeping differs between strategies | drop ledger mismatch |
| `litmus_fast_guard_outlives_concurrent_writers` | H6 (most dangerous): weakening the double-read/Slot orderings is benign on x86 | tag 1 freed behind live guards; paired with the structural `ordering_guard` unit tests that fail on any weakened `SeqCst` |

Mutation checks actually performed during the audit (then reverted):

* skipping the helping slot in `Debt::pay_all` → the BeforeConfirm schedule fails/hangs;
* weakening the confirming fast load to `Relaxed` and weakening `Debt::pay` → the
  `ordering_guard` unit tests fail deterministically even though runtime stress tests stay green
  on x86 (this is the key reason a structural guard is required);
* short-circuiting the GEN collision → the explicit `handover_ok` counter test fails, proving the
  seam really traverses the collision branch rather than passing by accident.

## 8. How to run

```sh
cargo fetch --locked
cargo test --features weak,internal-test-strategies,experimental-strategies
# one scenario in isolation
cargo test --features weak,internal-test-strategies,experimental-strategies --test litmus \
    litmus_fallback_collision_after_reserve
# structural ordering guards
cargo test --features weak,internal-test-strategies,experimental-strategies --lib ordering_guard
```

Outputs: ordinary `cargo test` output per binary; a failure prints the step trace, tag/provenance
identity, and per-tag drop histogram. No network, sleep, or machine-specific path is involved.
