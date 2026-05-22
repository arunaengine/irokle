// SPDX-License-Identifier: MIT OR Apache-2.0
//! Reopen an Iroh-backed node from Fjall storage using the same Iroh key.

use irokle::history::HistoryOrder;
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
#[irokle(type_id = "example.recovery.note")]
struct Note {
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secret_key = iroh::SecretKey::generate();
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_path_buf();

    let topic_id = {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .secret_key(secret_key.clone())
            .bind()
            .await?;
        let node = Irokle::builder()
            .with_net(endpoint)
            .with_fjall_path(&path)?
            .build()?;
        let topic = node.create_topic::<Note>(TopicConfig::default())?;
        topic.publish(Note {
            text: "persisted on disk".into(),
        })?;
        let topic_id = topic.id();
        node.endpoint().expect("iroh endpoint").close().await;
        topic_id
    };

    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(secret_key)
        .bind()
        .await?;
    let recovered = Irokle::builder()
        .with_net(endpoint)
        .with_fjall_path(&path)?
        .build()?;

    println!("recovered topics: {}", recovered.list_topics()?.len());
    let topic = recovered.open_topic::<Note>(topic_id)?;
    for record in topic.history(HistoryOrder::OldestFirst)? {
        println!("{}", record.event.text);
    }

    recovered.endpoint().expect("iroh endpoint").close().await;
    Ok(())
}
