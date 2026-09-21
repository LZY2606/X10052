# Unreleased (linearisation audit: debt helping, guard fallback, CAS)

This entry accompanies `ANALYSIS.md`, which traces pointer provenance through `load`,
`compare_and_swap`, `wait_for_readers`, guard `Drop` and `into_inner`, and identifies the
linearisation point of fast slots, the helping slot (including the collision/handover branch)
and the fallback.

## Added
- `tests/litmus.rs`: a deterministic litmus suite for the audit. Every scenario is a controlled
  interleaving (barriers/scoped joins plus an atomic rendezvous seam), never a random sleep; each
  one records its step outcomes and is cross-checked against the `RwLock<()>` reference strategy.
  Coverage: more than eight simultaneously held guards on one thread (fast-slot exhaustion), a
  writer replacing the storage while a reader is parked in the helping fallback at three distinct
  windows (AfterReserve, AfterLoad, BeforeConfirm), writer-vs-CAS contention with both a losing
  and a winning CAS, exact-once dropping of displaced values under mixed fast/fallback/promoted
  guards, pointer-provenance across allocator address reuse, and the guard-drop/`into_inner`
  balance in both directions.
- `src/strategy/test_ctl.rs` (feature `internal-test-strategies` only): the timing-free
  rendezvous used by the litmus suite, plus counters proving the handover collision branch is
  really entered, and a helper to read a storage's internal address for per-instance arming. The
  whole module is compiled out of normal builds.
- Three feature-gated `maybe_pause` seams in the existing hybrid/helping code
  (`src/strategy/hybrid.rs::fallback`, `src/debt/helping.rs::confirm`). No production ordering or
  code path changes: without the feature the compiled code is byte-for-byte the same algorithm.
- In-crate `ordering_guard` unit tests (`src/strategy/hybrid.rs`) that structurally pin the
  `SeqCst` orderings of the fast double read, `Debt::pay`, and writer pointer publication. These
  catch weakened orderings deterministically, which runtime tests on x86-TSO cannot.
- `ANALYSIS.md` with the entry/state/cache/error/output walkthrough and the full linearisation
  table.

## Implementation choices
- Reused the existing strategy abstraction for the reference oracle instead of writing a second
  engine: the suite is generic over `S: Strategy`/`S: CaS` and compares traces against
  `RwLock<()>`, with a third configuration (`FillFastSlots`) that forces every load through the
  helping fallback. This is cross-checking against an independent algorithm, not a parallel
  reimplementation of the hybrid strategy.
- Determinism over repetition: a global atomic state machine (`DISARMED/ARMED/ENTERED/RELEASED`)
  parks exactly one reader at a named window while the writer thread drives the schedule; seam
  users are serialised with a process-wide lock like `tests/stress.rs`.
- The collision seam is keyed by storage address and only reader calls park (writer-recursive
  loads observe ENTERED/RELEASED), so handover synthesis cannot self-deadlock even with fast
  slots disabled.
- Diagnostics are carried on every assertion (tag, pointer address, per-tag drop histogram) so a
  red test identifies the exact broken balance.

## Gaps in the previous coverage that this closes
- No prior test held more than eight guards on one thread *while a writer replaced the storage*
  and audited the exact drop count of both values (the old `lease_overflow` checked ref counts
  only on the unchanging value).
- The helping collision/handover path was exercised only by random stress tests; there was no
  deterministic schedule proving a reader parked between reservation and pointer load receives a
  live, provenance-correct value, and no test asserting the branch was entered at all.
- CAS coverage checked ref counts in a single-threaded loop (`cas_ref_cnt`) but never a
  controlled schedule where a competing swap lands strictly between the CAS's protected load and
  its `compare_exchange`, with the rejected `new` and displaced `old` ledgers audited separately.
- No test tied exact-once destruction to the fast/helping/`into_inner` mixture simultaneously,
  and none probed pointer provenance when the allocator recycles an address.
- The #198/#200/#204 ordering fixes had a regression test (`tests/bug-198.rs`) for one observed
  crash, but nothing guarded the other `SeqCst` sites against "performance" relaxations.

## Adjacent-semantics regression protection
- The full existing suite is unchanged and still passes under
  `--features weak,internal-test-strategies,experimental-strategies` (lib, random proptest,
  stress, doctests); the stress suite's `full_slots` strategy variant runs the same linked-list,
  unroll and parallel-load storms with fast slots disabled.
- Non-test builds (`cargo build`, `--features weak`) compile with no new warnings and contain no
  test code: every seam and the control module are `#[cfg(feature =
  "internal-test-strategies")]`.
- `Cache` and `Weak` paths are untouched; the audit records why they cannot move a linearisation
  point (Cache owns a full reference and only revalidates with a Relaxed comparison; Weak uses
  the same `RefCnt` protocol).
- `rustfmt` clean and `cargo clippy --lib --tests` reports no warnings in the changed code.

## Most dangerous counterexample and its regression
The most dangerous one is the lock-free reference replacement / debt helping / memory-ordering
triple at the fast path's *double read* and at a helping-slot reservation:

1. A reader publishes a debt (fast slot) or a reservation (helping GEN) and must perform a
   *second* SeqCst load of the storage before trusting the pointer; the guard must be built from
   that second load's provenance.
2. Concurrently the writer swaps the pointer (SeqCst) and, in `wait_for_readers`, bumps the
   strong count of every still-visible debt and resolves any half-published reservation by
   handing over an already protected value.
3. If the confirmation load or the debt/control publication is weakened, or if `wait_for_readers`
   omits the helping slot, the strong count can reach zero and `Arc::Drop` can free the payload
   while a reader is still dereferencing it — use-after-free. x86-TSO hides the ordering failure
   at runtime, so ordinary stress runs stay green, which is exactly how the original #198 class
   of bugs survived.

Regression coverage for this counterexample is deliberately two-pronged:
- `litmus_fast_guard_outlives_concurrent_writers` and
  `litmus_unprotected_reader_cannot_outlive_wait_for_readers` in `tests/litmus.rs` exercise the
  behaviour with controlled interleavings (verified: deleting the helping slot from `pay_all`
  makes the latter deterministically fail/hang);
- the in-crate structural tests
  `strategy::hybrid::ordering_guard::{fast_confirm_ordering_is_seqcst, debt_payment_is_seqcst_cas,
  swap_and_cas_linearisation_is_seqcst}` fail at test-build time if any of those orderings is
  relaxed (verified by mutation before restoring the code).

# 1.9.2

* Document RefCnt must not panic (#208).

# 1.9.1

* One more SeqCst :-| (#204).

# 1.9.0

* Promote certain orderings to SeqCst. Original proofs based on wrong reading of
  standard :-(. Expect some performance degradation (#198, #200).

# 1.8.2

* Proper gate of `Pin` (since 1.39 - we are not using only `Pin`, but also
  `Pin::into_inner`, #197).

# 1.8.1

* Some more careful orderings (#195).

# 1.8.0

* Support for Pin (#185, #183).
* Fix (hopefully) crash on ARM (#164).
* Fix Miri check (#186, #156).
* Fix support for Rust 1.31.0.
* Some minor clippy lints.

# 1.7.1

* Fix docs build (mutually exclusive features, #112).

# 1.7.0

* Support for no-std builds with the `experimental-thread-local`. Needs nightly
  compiler. No stability guarantees with this feature (#93).

# 1.6.0

* Fix a data race reported by MIRI.
* Avoid violating stacked borrows (AFAIK these are still experimental and not
  normative, but better safe than sorry). (#80).
* The `AccessConvert` wrapper is needed less often in practice (#77).

# 1.5.1

* bug: Insufficient synchronization on weak platforms (#76).

  Never observed in practice (it's suspected practical weak platforms like ARM
  are still stronger than the model), but still technically UB.
* docs: Mention triomphe's `ThinArc` around the fat-pointer limitations.

# 1.5.0

* Support serde (by a feature).

# 1.4.0

* Allow const-initializing ArcSwapOption (`const_empty` method).

# 1.3.2

* More helpful description of the `AsRaw` trait (isn't implemented for owned
  `Arc`/`Option<Arc>`).

# 1.3.1

* Cache doc improvements.

# 1.3.0

* Allow mapping of DynAccess.
* Fix some lints.
* Don't leave threads running in tests/doctests. It's a bad form and annoys
  miri.

# 1.2.0

* Miri and 32 bit tests in CI.
* Making the writers lock-free. Soft-removing the IndependentStrategy, as it is
  no longer needed (hidden and the same as the DafultStrategy).

# 1.1.0

* Fix soundness bug around access::Map. Technically a breaking change, but
  unlikely to bite and breaking seems to be the least bad option. #45.

# 1.0.0

* Remove Clone implementation. People are often confused by it and it is easy to
  emulate by hand in the rare case it is actually needed.

# 1.0.0-rc1

* Get rid of the `load_signal_safe`. It only complicates things and it is niche;
  signal-hook-registry has its own simplified version.
* Avoid `from_ptr(as_ptr())`. Slight change in `RefCnt::inc` which technically
  is API breaking change, but this one should not matter in practice.
* Extend documentation about clone behaviour.
* Few more traits for Guard (`From<T: RefCnt>`, `Default`).
* Get rid of `rcu_unwap`, the whole concept is a trap.
* Hide the whole gen lock thing.
* Introduce the `Strategy`, as a high level way to choose how exactly the
  locking happens.
  - Not possible to implement by downstream users just yet, or call them.
  - The CaS is its own trait for flexibility.
* Adding the SimpleGenLock experimental strategy.
  - Not part of stability guarantees.

# 0.4.7

* Rename the `unstable-weak` to `weak` feature. The support is now available on
  1.45 (currently in beta).

# 0.4.6

* Adjust to `Weak::as_ptr` from std (the weak pointer support, relying on
  unstable features).
* Support running on miri (without some optimizations), so dependencies may run
  miri tests.
* Little optimization when waiting out the contention on write operations.

# 0.4.5

* Added `Guard::from_inner`.

# 0.4.4

* Top-level docs rewrite (less rambling, hopefully more readable).

# 0.4.3

* Fix the `Display` implementation on `Guard` to correctly delegate to the
  underlying `Display` implementation.

# 0.4.2

* The Access functionality ‒ ability to pass a handle to subpart of held data to
  somewhere with the ability to update itself.
* Mapped cache can take `FnMut` as well as `Fn`.

# 0.4.1

* Mapped caches ‒ to allow giving access to parts of config only.

# 0.4.0

* Support for Weak pointers.
* RefCnt implemented for Rc.
* Breaking: Big API cleanups.
  - Peek is gone.
  - Terminology of getting the data unified to `load`.
  - There's only one kind of `Guard` now.
  - Guard derefs to the `Arc`/`Option<Arc>` or similar.
  - `Cache` got moved to top level of the crate.
  - Several now unneeded semi-internal traits and trait methods got removed.
* Splitting benchmarks into a separate sub-crate.
* Minor documentation improvements.

# 0.3.11

* Prevention against UB due to dropping Guards and overflowing the guard
  counter (aborting instead, such problem is very degenerate anyway and wouldn't
  work in the first place).

# 0.3.10

* Tweak slot allocation to take smaller performance hit if some leases are held.
* Increase the number of lease slots per thread to 8.
* Added a cache for faster access by keeping an already loaded instance around.

# 0.3.9

* Fix Send/Sync for Guard and Lease (they were broken in the safe but
  uncomfortable direction ‒ not implementing them even if they could).

# 0.3.8

* `Lease<Option<_>>::unwrap()`, `expect()` and `into_option()` for convenient
  use.

# 0.3.7

* Use the correct `#[deprecated]` syntax.

# 0.3.6

* Another locking store (`PrivateSharded`) to complement the global and private
  unsharded ones.
* Comparison to other crates/approaches in the docs.

# 0.3.5

* Updates to documentation, made it hopefully easier to digest.
* Added the ability to separate gen-locks of one ArcSwapAny from others.
* Some speed improvements by inlining.
* Simplified the `lease` method internally, making it faster in optimistic
  cases.

# 0.3.4

* Another potentially weak ordering discovered (with even less practical effect
  than the previous).

# 0.3.3

* Increased potentially weak ordering (probably without any practical effect).

# 0.3.2

* Documentation link fix.

# 0.3.1

* Few convenience constructors.
* More tests (some randomized property testing).

# 0.3.0

* `compare_and_swap` no longer takes `&Guard` as current as that is a sure way
  to create a deadlock.
* Introduced `Lease` for temporary storage, which doesn't suffer from contention
  like `load`, but doesn't block writes like `Guard`. The downside is it slows
  down with number of held by the current thread.
* `compare_and_swap` and `rcu` uses leases.
* Made the `ArcSwap` as small as the pointer itself, by making the
  shards/counters and generation ID global. This comes at a theoretical cost of
  more contention when different threads use different instances.

# 0.2.0

* Added an `ArcSwapOption`, which allows storing NULL values (as None) as well
  as a valid pointer.
* `compare_and_swap` accepts borrowed `Arc` as `current` and doesn't consume one
  ref count.
* Sharding internal counters, to improve performance on read-mostly contented
  scenarios.
* Providing `peek_signal_safe` as the only async signal safe method to use
  inside signal handlers. This removes the footgun with dropping the `Arc`
  returned from `load` inside a signal handler.

# 0.1.4

* The `peek` method to use the `Arc` inside without incrementing the reference
  count.
* Some more (and hopefully better) benchmarks.

# 0.1.3

* Documentation fix (swap is *not* lock-free in current implementation).

# 0.1.2

* More freedom in the `rcu` and `rcu_unwrap` return types.

# 0.1.1

* `rcu` support.
* `compare_and_swap` support.
* Added some primitive benchmarks.

# 0.1.0

* Initial implementation.
