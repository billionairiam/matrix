use std::any::Any;
use std::collections::HashMap;
use std::sync::{
    OnceLock, RwLock,
    atomic::{AtomicBool, Ordering},
};

pub const GETIP: &str = "GETIP";
pub const NEW_BINANCE_TRADER: &str = "NEW_BINANCE_TRADER";
pub const NEW_ASTER_TRADER: &str = "NEW_ASTER_TRADER";
pub const SET_HTTP_CLIENT: &str = "SET_HTTP_CLIENT";

// Arguments are a slice of dynamic boxes.
// Return value is an Option containing a dynamic box.
pub type HookArgs = [Box<dyn Any + Send + Sync>];
pub type HookResult = Option<Box<dyn Any + Send + Sync>>;

// The function signature for a hook
// It must be Send + Sync to be stored in a global variable
pub type HookFunc = Box<dyn Fn(&HookArgs) -> HookResult + Send + Sync>;

static ENABLE_HOOKS: AtomicBool = AtomicBool::new(true);

// Thread-safe global map using OnceLock (standard in Rust 1.70+)
fn get_hooks() -> &'static RwLock<HashMap<String, HookFunc>> {
    static HOOKS: OnceLock<RwLock<HashMap<String, HookFunc>>> = OnceLock::new();
    HOOKS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Enable or Disable hooks globally
pub fn set_hooks_enabled(enabled: bool) {
    ENABLE_HOOKS.store(enabled, Ordering::Relaxed);
}

/// Register a new hook
pub fn register_hook<F>(key: &str, func: F)
where
    F: Fn(&HookArgs) -> HookResult + Send + Sync + 'static,
{
    let hooks_map = get_hooks();
    let mut write_guard = hooks_map.write().expect("Failed to acquire write lock");
    write_guard.insert(key.to_string(), Box::new(func));
}

/// Execute a hook and cast the result to type T
pub fn hook_exec<T: 'static + Any + Send + Sync>(
    key: &str,
    args: Vec<Box<dyn Any + Send + Sync>>,
) -> Option<T> {
    // 1. Check if hooks are enabled
    if !ENABLE_HOOKS.load(Ordering::Relaxed) {
        println!("🔌 Hooks are disabled, skip hook: {}", key);
        return None;
    }

    let hooks_map = get_hooks();
    let read_guard = hooks_map.read().expect("Failed to acquire read lock");

    // 2. Look up the hook
    if let Some(hook) = read_guard.get(key) {
        println!("🔌 Execute hook: {}", key);

        // 3. Execute the closure
        let result_any = hook(&args);

        // 4. Downcast the result
        match result_any {
            Some(boxed_val) => match boxed_val.downcast::<T>() {
                Ok(val) => return Some(*val),
                Err(_) => {
                    println!(
                        "🔌 Hook executed, but return type mismatched for key: {}",
                        key
                    );
                    return None;
                }
            },
            None => return None,
        }
    } else {
        println!("🔌 Do not find hook: {}", key);
    }

    None
}

// Makes creating arguments easier: args![userid, client]
#[macro_export]
macro_rules! args {
    ($($x:expr),* $(,)?) => {
        vec![
            $(
                Box::new($x) as Box<dyn std::any::Any + Send + Sync>
            ),*
        ]
    };
}
