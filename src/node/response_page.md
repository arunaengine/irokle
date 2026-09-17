<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
One bounded page of `request`; see [`crate::sync::SyncEngine::response_page`].

A transport repeats request and page until the page reports no more:

```
use irokle::sync::{PageBudget, RequestKnowledge, SyncCredit, SyncData};
use irokle::{Ed25519Signer, Irokle, Storage, TopicConfig};

#[derive(Clone, irokle::Event, serde::Deserialize, serde::Serialize)]
#[irokle(type_id = "doc.note")]
struct Note(u32);

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let alice = Irokle::builder().with_signer(Ed25519Signer::from_bytes(&[1; 32])).build()?;
let bob = Irokle::builder().with_signer(Ed25519Signer::from_bytes(&[2; 32])).build()?;
let topic = alice.create_topic::<Note>(TopicConfig {
    initial_peers: [bob.peer_id()].into(),
    ..TopicConfig::default()
})?;
// Bob holds the genesis first; pages then carry the events.
let genesis = alice.plan_sync_data(bob.peer_id(), &bob.sync_summary(topic.id())?)?;
bob.receive_sync_outcome(alice.peer_id(), genesis)?;
for index in 0..10 {
    topic.publish(Note(index))?;
}
let mut pages = 0;
let mut knowledge = RequestKnowledge::default();
loop {
    let summary = alice.sync_summary(topic.id())?;
    let mut request = bob.plan_request_with(alice.peer_id(), &summary, &knowledge)?;
    if request.wants.is_empty() && request.actor_range_hints.is_empty() {
        break;
    }
    request.credit = SyncCredit { ops: 4, ..request.credit };
    let page = alice.response_with(
        bob.peer_id(),
        &request,
        PageBudget::from_credit(request.credit),
        &bob.sync_summary(topic.id())?,
    )?;
    assert!(page.ops.len() <= 4);
    let received = !page.ops.is_empty();
    let data = SyncData { topic_id: topic.id(), ops: page.ops };
    bob.receive_sync_outcome(alice.peer_id(), data)?;
    knowledge.settle(
        &request.window,
        (&page.positions, page.continued),
        (received, summary.actor_clock.iter().count()),
    );
    pages += 1;
}
assert_eq!(pages, 3);
assert_eq!(bob.storage().actor_clock(&topic.id())?, alice.storage().actor_clock(&topic.id())?);
# Ok(())
# }
```
