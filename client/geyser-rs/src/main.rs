use std::{
    fs,
    sync::{Arc, atomic::AtomicU64},
};

use dotenv::dotenv;
use solana_stream_sdk::GeyserSubscribeUpdate;
use tokio::sync::mpsc;

use crate::{
    handlers::processor::process_updates,
    runtime::{
        runner::run_geyser_stream, settings::Settings, subscription::build_subscribe_request,
    },
    utils::{
        blocktime::{BlockTimeCache, create_transactions_by_slot, latency_monitor_task},
        config::Config,
    },
};

mod handlers;
mod runtime;
mod utils;

const UPDATE_CHANNEL_CAPACITY: usize = 10_000;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenv().ok();
    env_logger::init();

    let settings = Settings::from_env()?;
    let config_content = fs::read_to_string(&settings.config_path)?;
    let config: Config = serde_jsonc::from_str(&config_content)?;
    let request = build_subscribe_request(&config);

    let transactions_by_slot = create_transactions_by_slot();
    let block_time_cache = BlockTimeCache::new(&settings.rpc_endpoint);
    let tracked_slot = Arc::new(AtomicU64::new(0));
    let (updates_tx, updates_rx) = mpsc::channel::<GeyserSubscribeUpdate>(UPDATE_CHANNEL_CAPACITY);

    let latency_handle = {
        let block_time_cache = block_time_cache.clone();
        let transactions_by_slot = transactions_by_slot.clone();
        tokio::spawn(async move {
            latency_monitor_task(block_time_cache, transactions_by_slot).await;
        })
    };

    let processor_handle = {
        let transactions_by_slot = transactions_by_slot.clone();
        tokio::spawn(async move {
            process_updates(updates_rx, transactions_by_slot).await;
        })
    };

    let geyser_handle = {
        let tracked_slot = tracked_slot.clone();
        let updates_tx = updates_tx.clone();
        tokio::spawn(run_geyser_stream(
            settings.grpc_endpoint.clone(),
            settings.x_token.clone(),
            request,
            tracked_slot,
            updates_tx,
        ))
    };

    tokio::try_join!(latency_handle, processor_handle, geyser_handle)?;

    Ok(())
}
