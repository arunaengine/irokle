// SPDX-License-Identifier: MIT OR Apache-2.0
//! Compiling form of the Iroh runtime configuration shown in the README.

use irokle::net::IrohRuntimeConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![irokle::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await?;

    let runtime = IrohRuntimeConfig {
        connect_timeout: Duration::from_secs(10),
        sync_io_timeout: Duration::from_secs(10),
        resync_interval: Duration::from_secs(15),
        ..IrohRuntimeConfig::default()
    };

    let node = irokle::Irokle::builder()
        .with_iroh_runtime_config(runtime)
        .with_net(endpoint)
        .build()?;

    println!("iroh runtime configured for peer {}", node.peer_id());

    Ok(())
}
