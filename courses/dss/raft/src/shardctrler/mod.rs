pub mod client;
#[cfg(test)]
pub mod config;
pub mod errors;
pub mod server;
#[cfg(test)]
mod tests;

pub const N_SHARDS: usize = 10;
