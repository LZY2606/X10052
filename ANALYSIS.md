# arc-swap：debt helping、guard fallback 与 CAS 的线性化点审计

本文档是对 `vorner/arc-swap`（当前工作区版本 1.9.2）无锁引用替换机制的审计记录。
审计主线是 **pointer provenance**：一个指针从 `Arc` 被冻结进 `AtomicPtr`，到被读者登记为
debt、被写者代偿（helping/pay）、最终作为 `Guard` 或 `T` 交还给调用方的完整生命周期。
配套的可执行反证在 `tests/litmus.rs`，实现选择与覆盖空白记录在 `CHANGELOG.md`。

## 1. 入口（Entry）

所有公开入口最终都收敛到 `strategy::sealed::{InnerStrategy, CaS}` 的两个半原语上
（`src/strategy/hybrid.rs`、`src/strategy/mod.rs`）：

| 公开入口 | 位置 | 收敛到的策略原语 |
| --- | --- | --- |
| `ArcSwapAny::load` / `load_full` | `src/lib.rs` | `InnerStrategy::load` → `HybridProtection::attempt`（fast）或 `fallback`（helping） |
| `ArcSwapAny::store` / `swap` | `src/lib.rs` | `AtomicPtr::swap(SeqCst)` + `wait_for_readers` → `Debt::pay_all` |
| `ArcSwapAny::compare_and_swap` / `rcu` | `src/lib.rs` | `CaS::compare_and_swap`（load + `compare_exchange_weak` 循环） |
| `ArcSwapAny::drop` / `into_inner` | `src/lib.rs` | `wait_for_readers` + `T::dec` / `T::from_ptr` |
| `Guard::into_inner` / `Guard` 的 `Drop` | `src/lib.rs`、`src/strategy/hybrid.rs` | `HybridProtection::{into_inner, drop}` |
| `Cache::load` / `revalidate` | `src/cache.rs` | Relaxed 指针比较，过期时走 `load_full`（即上面的 load 路径） |

## 2. 状态（State）

审计涉及四类共享状态，各自的合法取值与不变量：

- **storage**（`ArcSwapAny.ptr: AtomicPtr<T::Base>`）：任意时刻持有一个有效指针，且该指针
  自带 1 个引用计数（"storage 自己的那一份"）。替换时这份计数随旧指针带出。
- **fast slots**（`src/debt/fast.rs`，每线程节点 8 个 `Debt`）：取值为 `Debt::NONE`(=0b11)
  或一个指针。只有属主线程能把 `NONE` 改成指针（`swap(SeqCst)`），写者只能把指针
  `pay` 回 `NONE`（`compare_exchange(SeqCst)`）。不变量：slot 中的指针表示"欠一个引用计数"。
- **helping slot**（`src/debt/helping.rs`，每线程节点 1 组）：`control` 三态
  `IDLE | GEN_TAG|generation | REPLACEMENT_TAG|envelope_ptr`，配套 `active_addr`（读者
  声明要读哪个 storage）、`slot`（一个普通 `Debt`）、`handover`/`space_offer`（写者向读者
  递交已保护指针的信封，靠 `#[repr(align(4))]` 腾出 tag 位）。
- **节点链表**（`src/debt/list.rs`）：prepend-only 的 `Node` 链表，`in_use` 三态
  `UNUSED/USED/COOLDOWN`，`active_writers` 引用计数式预留。节点永不释放；线程退出时
  节点进入 cooldown，供后来的线程认领。generation 回绕（`usize::MAX/4` 次一次）时属主
  主动把节点送入 cooldown，消除 helping 的 ABA 回绕窗口。

## 3. Pointer provenance 追踪

### 3.1 `load`（fast slot 路径）

`HybridProtection::attempt`（`src/strategy/hybrid.rs`）：

1. `ptr = storage.load(SeqCst)` —— 候选指针。
2. `debt = node.new_fast(ptr)` —— 把候选登记进本线程 fast slot（`swap(SeqCst)`）。
   从这一刻起，任何写者的 `pay_all` 都会替我们代偿这个指针的引用计数。
3. `confirm = storage.load(SeqCst)` —— 确认。三个分支：
   - `ptr == confirm`：**成功**。注意代码刻意使用 `confirm` 而非 `ptr` 构造返回值
     （注释明确说明：地址相等不代表 provenance 相同，指针可能已被释放并被分配器
     复用）。返回的 `Guard` 持有 `debt`，不持有引用计数。
   - `ptr != confirm` 且 `debt.pay(ptr)` 成功：我们自己把刚登记的 debt 退掉，本次
     尝试作废，转入 fallback。**未线性化**。
   - `ptr != confirm` 但 `debt.pay(ptr)` 失败：说明写者已经替我们代偿了 `ptr` 的
     引用计数 —— 我们实际已经"拥有"一份受保护的 `ptr`。直接以无 debt 形式返回
     （`Self::new(ptr, None)`）。

### 3.2 `load`（helping fallback 路径）

`HybridProtection::fallback` + `src/debt/helping.rs`：

1. `gen = node.new_helping(storage_addr)`：`active_addr` 发布目标 storage 地址，
   `control` 从 `IDLE` `swap` 为带 `GEN_TAG` 的 generation（两处都是 SeqCst）。
2. `candidate = storage.load(SeqCst)`。
3. `confirm_helping(gen, candidate)`：先把 `candidate` 写入 helping `slot`
   （`swap(SeqCst)`），再把 `control` `swap` 回 `IDLE`：
   - 读回的 `control == gen`：无人干预，debt 确认成功。随后**立即** `into_inner`
     （inc + pay 掉自己的 debt），所以 fallback 产出的 `Guard` 一律是"已拥有引用
     计数、不占用 slot"的形态 —— 这是同线程可以持有任意多个 fallback guard 的原因。
   - 读回的 `control` 带 `REPLACEMENT_TAG`：写者已经 `help` 过我们。信封里是写者
     加载并 **预先 inc 过** 的指针。我们把刚登记的 debt 原样退回（`pay` 失败说明
     写者连这份也代偿了，则 `T::dec` 对冲），然后使用写者递交的指针。

### 3.3 `compare_and_swap`

`CaS::compare_and_swap`（`src/strategy/hybrid.rs`）的循环：

1. `old = load(storage)` —— 走 3.1/3.2 完整保护流程。
2. `old.as_ptr() != current.as_raw()` → 直接返回 `old`（表现为一次 `load_full`）。
3. `storage.compare_exchange_weak(current, new, SeqCst, Relaxed)` 成功 →
   `T::into_ptr(new)` 把 `new` 的计数交给 storage；`wait_for_readers(old)` 代偿所有
   残留 debt；`T::dec(old)` 抵消从 storage 带出的那份计数（返回给调用方的 `old`
   guard 还握着一份）。失败则带着 `new` 重试。

### 3.4 `wait_for_readers` / `Debt::pay_all`

写者替换指针后（`swap`/`compare_exchange`/析构），`Debt::pay_all`
（`src/debt/mod.rs`）：

1. 对旧指针 `T::from_ptr` + 预支一次 `T::inc`（第一笔代偿的"弹药"）。
2. 遍历节点链表：每个节点先 `reserve_writer`（把节点钉在 cooldown 之外），再
   `local.help(node, storage_addr, &replacement)` 帮助处于 `GEN_TAG` 状态的读者
   （写者自己 `load` 一份新值，inc 后通过信封 CAS 进对方 `control`），最后逐个
   `slot.pay(ptr)`；每成功代偿一笔就再 `T::inc` 一次预支下一笔。
3. 收尾 drop 掉临时的 `val`，净效果：每笔被代偿的 debt 恰好 +1 引用计数，所有权
   转移给对应的读者。

### 3.5 Guard drop 与 `Guard::into_inner`

`HybridProtection::drop` / `into_inner`（`src/strategy/hybrid.rs`）：

- drop：有 debt 则 `debt.pay(ptr)`；成功（debt 还没被代偿）→ 直接结束，引用计数
  全程没动过；失败（写者已代偿）→ 我们其实握着一份计数，走 `ManuallyDrop::drop`
  即 `T::dec`。无 debt → 直接 `dec`。
- `into_inner`：有 debt 则先 `T::inc` 再 `pay`；`pay` 失败说明写者也代偿了一份，
  多出来的一份立即 `T::dec` 对冲。无 debt 则直接交出内部的 `T`。

### 3.6 输出（Output）

- `load` → `Guard`：fast 成功时 provenance 来自 **confirm 那次 load**；debt 已被
  代偿时来自第一次 load；fallback 时来自 candidate（自我确认）或写者信封（被帮助）。
- `load_full` / `Guard::into_inner` → `T`：保证是 owned 引用计数，可脱离 `ArcSwap` 存活。
- `compare_and_swap` → `Guard`：成功时是旧值的 guard（线性化于 compare_exchange）；
  失败时是当前值的 guard（线性化于观察到不等的那次 load）。
- `swap` → `T`：storage 带出的那份旧值计数。

## 4. 线性化点判定

| 路径 | 线性化点 | 依据 |
| --- | --- | --- |
| fast slot 成功 | 第二次 `storage.load(SeqCst)`（confirm） | debt 发布（slot 的 SeqCst swap）先于 confirm；写者的 `swap(SeqCst)` 与两次 load 在同一全序中，故写者要么看到 debt 并代偿，要么读者看到新值 |
| fast slot "debt already paid" | 第一次 `storage.load(SeqCst)` | 写者的 `pay_all` 已把该指针的计数补上，保护自第一次 load 起成立 |
| fast slot 失败（自行退 debt） | 不线性化，重试/fallback | 状态已复原 |
| fallback 未被帮助 | `control.swap(IDLE)` 确认成功点（候选值由 candidate load 提供） | `control` 的 SeqCst swap 与写者的 storage swap 定序；写者若已替换，必然在 `control` 上留下 REPLACEMENT 或代偿 slot |
| fallback 被帮助 | 写者 `control.compare_exchange(gen → REPLACEMENT)` 成功点 | 读者从信封取得写者已 inc 的指针，所有权随信封移交 |
| `compare_and_swap` 成功 | `compare_exchange_weak` 成功点 | 唯一的原子 RMW，天然是线性化点 |
| `compare_and_swap` 失败 | 观察到 `old != current` 的那次 load | 与 `load` 相同 |
| 写者代偿 | 每笔 `Debt::pay` 的 `compare_exchange(SeqCst)` | 与读者 slot 的 SeqCst swap 构成同步对；见第 7 节的内存序反例 |
| guard drop / `into_inner` | `Debt::pay` 成功点（自行归还）或写者的代偿点 | 二者必居其一，且只居其一 —— 恰好一次 |

## 5. 缓存（Cache）与相邻语义

`src/cache.rs` 的 `Cache::revalidate` 用 `Relaxed` load 比较缓存指针与 storage 指针，
只有不等时才走完整的 `load_full`。这是安全的，因为缓存本身已持有一份 owned 计数，
Relaxed 比较不承载同步语义；真正的同步发生在重新 `load_full` 时。代价是**延迟回收**：
缓存持有的旧值要到下一次 revalidate 才释放 —— 这是相邻语义中有意为之的退化，
litmus 套件中的 exactly-once 断言都以"所有 guard/cache 析构之后"为观察点，避免把
延迟回收误判为泄漏。`access.rs` 的 `Map` 投影只是 `load` 之上的组合，不引入新的
provenance 来源。

## 6. 错误传播与诊断

- 库内部不使用 `Result` 报错；协议违例通过 `debug_assert!`（如 slot 残留非 `NONE`、
  `control` 非 `IDLE`）和带数值的 `unreachable!("Invalid control value {:X}")` 暴露，
  保留现场值。
- `RwLock` 测试策略（`src/strategy/rw_lock.rs`）对锁中毒显式
  `expect("We don't panic in here")`，中毒即测试基础设施问题而非被测逻辑问题。
- litmus 套件的所有断言都带标签上下文（策略名、代际 id、drop 次数），`Registry`
  容量耗尽会报出容量与代际 id；线程 panic 经 `join().expect(...)` 保留原始 panic
  信息向上传播。

## 7. 最危险反例（无锁引用替换 / debt helping / 内存序）

**反例：代偿的 `fetch_add` 对析构的 `fetch_sub` 不可见 → use-after-free。**
读者登记 debt 后，写者 `pay` 成功并 `T::inc` 代偿；若 `Debt::pay` 的内存序弱于
SeqCst/Acquire，读者线程里 `Arc` 析构的 `fetch_sub(1)` 可能在写者代偿的
`fetch_add(1)` 对本线程可见 **之前** 就观察到"自己是最后一个引用"，于是提前释放
内存 —— 而写者/其他读者随后就在这块已释放的内存上增减计数。这正是 1.9.0 系列
（#198/#200/#204）修复的序弱问题，`Debt::pay` 失败分支上的注释明确记录了这一点：
`Arc` 自身的 Acquire fence 位于 `fetch_sub` 之后，无法覆盖这个窗口。

**回归用例**：
- `churn_forced_fallback`（`tests/litmus.rs`）：用 `FillFastSlots` 把 **每一次** load
  都逼进 helping fallback，写者每轮 `store` 都触发 `pay_all`/`help`，在屏障锁步下
  高频重放"代偿 vs 析构"窗口；任何一次错误的恰好一次计数都会以 double-drop /
  泄漏形式被 `Registry` 捕获（在 miri 下则直接表现为 use-after-free）。
- `guards_beyond_fast_slots_use_fallback`：钉死 fast slot 耗尽后的精确计数账
  （8 个 debt + 其余 owned），代偿多一笔少一笔都会立即破坏断言。
- `cas_exactly_one_winner_*` / `store_vs_cas_race_*`：CAS 与写者竞争下的线性化
  反证 —— 两个 CAS 同 `current` 必须恰好一个赢；失败的 CAS 必须观察到顶替它的值。

次危险反例（同样留有回归保护）：
- **ABA / 指针复用 provenance**：confirm 与 ptr 地址相等但 provenance 不同
  （释放后复用）。代码用 `confirm` 构造返回值；miri 的 stacked-borrows 检查是其
  回归保护。
- **generation 回绕 ABA**：`usize` 回绕后写者可能把过期信封递给新读者。由
  cooldown 机制（`start_cooldown`/`check_cooldown` + `active_writers` 预留）防护；
  单测无法现实地回绕 `usize`，该项依赖代码审计与 `NodeReservation` 的不变量。

## 8. Litmus 对照表（每个用例反证一条设计假设）

| 用例（`tests/litmus.rs`） | 被反证的设计假设 | 假设破裂时的可观察后果 |
| --- | --- | --- |
| `guards_beyond_fast_slots_use_fallback` | "每线程 8 个 fast slot 永远够用 / fallback 不需要精确计数" | 第 9 个 guard 起计数账目错误；旧值 double-drop 或泄漏 |
| `guard_into_inner_releases_debt` | "`into_inner` 可以不归还 debt" | slot 泄漏（后续 fallback 行为异常）或重复代偿 |
| `churn_default_strategy` / `churn_forced_fallback` / `churn_rw_lock_cross_check` | "debt helping 交接的指针必然恰好保护一次" | 读者看到野值、double-drop、泄漏；与 RwLock 策略行为分叉 |
| `cas_exactly_one_winner_{default,forced_fallback,rw_lock}` | "一次 CAS 可以线性化两次" | 两个 CAS 都成功或都失败；终值不是胜者的值 |
| `store_vs_cas_race_{default,forced_fallback,rw_lock}` | "失败的 CAS 可以观察到与顶替者无关的值" | 失败 CAS 返回既非 `current` 也非新 store 的值 |
| `step_trace_matches_rw_lock` | "debt 机制不改变可观察语义" | 同一确定性脚本下，hybrid/forced-fallback 与 RwLock 的逐步轨迹分叉 |

所有用例只用 `Barrier` 做相位同步，不用 sleep、不访问外网、不依赖本机路径或特定
fixture 名称；每个用例可单独定位，例如
`cargo test --features internal-test-strategies --test litmus churn_forced_fallback`。
