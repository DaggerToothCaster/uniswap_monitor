pub mod trading_operations;
pub mod token_operations;
pub mod event_operations;
pub mod wallet_operations;
pub mod stats_operations;
pub mod metadata_operations;
pub mod system_operations;
pub mod price_operations;
pub mod usd_calculator;

// Re-export all operations for backward compatibility
pub use trading_operations::*;
pub use token_operations::*;
pub use event_operations::*;
pub use wallet_operations::*;
pub use stats_operations::*;
pub use metadata_operations::*;
pub use system_operations::*;
pub use price_operations::*;
pub use usd_calculator::*;

// Event type constants
pub const EVENT_TYPE_SWAP: &str = "swap";
pub const EVENT_TYPE_MINT: &str = "mint";
pub const EVENT_TYPE_BURN: &str = "burn";
pub const EVENT_TYPE_FACTORY: &str = "factory";
pub const EVENT_TYPE_UNIFIED: &str = "unified";
