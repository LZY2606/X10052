use arc_swap::litmus;
use arc_swap::{ArcSwapAny, DefaultStrategy};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
struct T { id:u32, d: Arc<AtomicUsize> }
impl Drop for T { fn drop(&mut self){ eprintln!("DROP id={}", self.id); self.d.fetch_add(1,Ordering::SeqCst);} }
fn token(id:u32) -> (Arc<T>, Arc<AtomicUsize>) {
    let d=Arc::new(AtomicUsize::new(0));
    (Arc::new(T{id,d:d.clone()}),d)
}
fn main() {
    litmus::reset();
    let (v, vd) = token(70);
    let shared = Arc::new(ArcSwapAny::<Arc<T>, DefaultStrategy>::from(v));
    let quiet = shared.load();
    drop(quiet);
    let (new, new_drops) = token(71);
    let guard = shared.load();
    shared.swap(new);
    eprintln!("vd after swap={}", vd.load(Ordering::SeqCst));
    drop(shared);
    eprintln!("vd after shared={} new={}", vd.load(Ordering::SeqCst), new_drops.load(Ordering::SeqCst));
    drop(guard);
    eprintln!("vd after guard={}", vd.load(Ordering::SeqCst));
    eprintln!("FINAL new_drops={}", new_drops.load(Ordering::SeqCst));
}
