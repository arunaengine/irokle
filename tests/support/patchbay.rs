use std::collections::BTreeSet;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use irokle::history::HistoryOrder;
use irokle::{Event, Irokle, PeerId, Storage, TopicConfig, TopicId};
use serde::{Deserialize, Serialize};

pub type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[ctor::ctor(unsafe)]
fn init_patchbay_userns() {
    // patchbay requires user namespace setup before Tokio starts worker threads.
    unsafe { ::patchbay::init_userns_for_ctor() }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Event)]
struct PatchNote {
    text: String,
}

#[derive(Clone)]
pub struct PatchNode {
    pub node: Irokle,
    addr: iroh::EndpointAddr,
}

pub struct PatchLab {
    pub lab: ::patchbay::Lab,
    router: ::patchbay::Router,
    pub alice_dev: ::patchbay::Device,
    pub bob_dev: ::patchbay::Device,
    pub carol_dev: ::patchbay::Device,
    pub alice: PatchNode,
    pub bob: PatchNode,
    pub carol: PatchNode,
}

impl PatchLab {
    pub async fn new() -> TestResult<Self> {
        let lookup = iroh::address_lookup::memory::MemoryLookup::new();
        let lab = ::patchbay::Lab::builder()
            .label("irokle-patchbay-sync")
            .build()
            .await?;
        let router = lab
            .add_router("public")
            .preset(::patchbay::RouterPreset::Public)
            .build()
            .await?;
        let alice_dev = lab
            .add_device("alice")
            .iface("eth0", router.id())
            .build()
            .await?;
        let bob_dev = lab
            .add_device("bob")
            .iface("eth0", router.id())
            .build()
            .await?;
        let carol_dev = lab
            .add_device("carol")
            .iface("eth0", router.id())
            .build()
            .await?;
        let alice = spawn_node(&alice_dev, lookup.clone()).await?;
        let bob = spawn_node(&bob_dev, lookup.clone()).await?;
        let carol = spawn_node(&carol_dev, lookup.clone()).await?;
        for node in [&alice, &bob, &carol] {
            lookup.add_endpoint_info(node.addr.clone());
        }
        Ok(Self {
            lab,
            router,
            alice_dev,
            bob_dev,
            carol_dev,
            alice,
            bob,
            carol,
        })
    }

    pub fn whitelist_all(&self) -> irokle::Result<()> {
        let peers = [
            self.alice.node.peer_id(),
            self.bob.node.peer_id(),
            self.carol.node.peer_id(),
        ];
        self.alice.node.add_peers_to_whitelist(peers)?;
        self.bob.node.add_peers_to_whitelist(peers)?;
        self.carol.node.add_peers_to_whitelist(peers)?;
        Ok(())
    }

    pub async fn impair_bob_link(&self) -> TestResult<()> {
        self.lab
            .set_link_condition(
                self.bob_dev.id(),
                self.router.id(),
                Some(::patchbay::LinkCondition::Manual(::patchbay::LinkLimits {
                    rate_kbit: 384,
                    loss_pct: 8.0,
                    latency_ms: 80,
                    jitter_ms: 25,
                    ..::patchbay::LinkLimits::default()
                })),
            )
            .await?;
        self.bob_dev
            .iface("eth0")
            .unwrap()
            .set_condition(
                ::patchbay::LinkCondition::Manual(::patchbay::LinkLimits {
                    loss_pct: 4.0,
                    latency_ms: 40,
                    ..::patchbay::LinkLimits::default()
                }),
                ::patchbay::LinkDirection::Both,
            )
            .await?;
        Ok(())
    }

    fn device_for(&self, peer_id: PeerId) -> TestResult<&::patchbay::Device> {
        if peer_id == self.alice.node.peer_id() {
            Ok(&self.alice_dev)
        } else if peer_id == self.bob.node.peer_id() {
            Ok(&self.bob_dev)
        } else if peer_id == self.carol.node.peer_id() {
            Ok(&self.carol_dev)
        } else {
            Err(io_error("unknown peer").into())
        }
    }
}

async fn spawn_node(
    dev: &::patchbay::Device,
    lookup: iroh::address_lookup::memory::MemoryLookup,
) -> TestResult<PatchNode> {
    let handle = dev.spawn(|dev| async move {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .address_lookup(lookup)
            .alpns(vec![irokle::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await?;
        let node = Irokle::builder().with_net(endpoint).build()?;
        let endpoint = node.endpoint().unwrap();
        let addr = ready_addr(endpoint).await?;
        let port = addr
            .ip_addrs()
            .next()
            .ok_or_else(|| io_error("iroh endpoint did not expose an IP address"))?
            .port();
        let ip = dev
            .ip()
            .ok_or_else(|| io_error("patchbay device has no IPv4 address"))?;
        let addr =
            iroh::EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::new(ip.into(), port));
        TestResult::Ok(PatchNode { node, addr })
    })?;
    handle.await?
}

async fn ready_addr(endpoint: &iroh::Endpoint) -> TestResult<iroh::EndpointAddr> {
    use iroh::Watcher;

    let addr = endpoint.addr();
    if !addr.addrs.is_empty() {
        return Ok(addr);
    }
    let mut stream = endpoint.watch_addr().stream();
    let addr = tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let addr = stream
                .next()
                .await
                .ok_or_else(|| io_error("iroh address watch ended"))?;
            if !addr.addrs.is_empty() {
                return TestResult::Ok(addr);
            }
        }
    })
    .await??;
    Ok(addr)
}

pub async fn publish_initial_topic(
    peers_dev: &::patchbay::Device,
    owner: &PatchNode,
    peers: impl IntoIterator<Item = PeerId>,
) -> TestResult<TopicId> {
    let initial_peers = peers.into_iter().collect::<BTreeSet<_>>();
    publish_initial_topic_with_config(
        peers_dev,
        owner,
        TopicConfig {
            initial_peers,
            ..TopicConfig::default()
        },
    )
    .await
}

pub async fn publish_initial_topic_with_config(
    peers_dev: &::patchbay::Device,
    owner: &PatchNode,
    config: TopicConfig,
) -> TestResult<TopicId> {
    let owner = owner.node.clone();
    in_device(peers_dev, move || async move {
        let topic = owner.create_topic::<PatchNote>(config)?;
        TestResult::Ok(topic.id())
    })
    .await
}

pub async fn publish(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
    text: &str,
) -> TestResult<()> {
    let node = owner.node.clone();
    let text = text.to_owned();
    in_device(dev, move || async move {
        node.open_topic::<PatchNote>(topic_id)?
            .publish(PatchNote { text })?;
        Ok(())
    })
    .await
}

pub async fn add_peer(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
    peer: PeerId,
) -> TestResult<()> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        node.open_topic::<PatchNote>(topic_id)?.add_peer(peer)?;
        Ok(())
    })
    .await
}

pub async fn remove_peer(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
    peer: PeerId,
) -> TestResult<()> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        node.open_topic::<PatchNote>(topic_id)?.remove_peer(peer)?;
        Ok(())
    })
    .await
}

pub async fn leave(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
) -> TestResult<()> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        node.open_topic::<PatchNote>(topic_id)?.leave()?;
        Ok(())
    })
    .await
}

pub async fn sync(
    dev: &::patchbay::Device,
    from: &PatchNode,
    to: &PatchNode,
    topic_id: TopicId,
) -> TestResult<()> {
    let node = from.node.clone();
    let addr = to.addr.clone();
    in_device(dev, move || async move {
        node.sync_addr_now(addr, topic_id).await?;
        Ok(())
    })
    .await
}

pub async fn sync_topic(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
) -> TestResult<()> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        node.sync_topic_now(topic_id).await?;
        Ok(())
    })
    .await
}

pub async fn isolate(dev: &::patchbay::Device) -> TestResult<()> {
    dev.iface("eth0")
        .unwrap()
        .set_condition(
            ::patchbay::LinkCondition::Manual(::patchbay::LinkLimits {
                loss_pct: 100.0,
                ..::patchbay::LinkLimits::default()
            }),
            ::patchbay::LinkDirection::Both,
        )
        .await?;
    Ok(())
}

pub async fn restore(dev: &::patchbay::Device) -> TestResult<()> {
    dev.iface("eth0")
        .unwrap()
        .clear_condition(::patchbay::LinkDirection::Both)
        .await?;
    Ok(())
}

pub async fn converge_pair(
    a_dev: &::patchbay::Device,
    a: &PatchNode,
    b_dev: &::patchbay::Device,
    b: &PatchNode,
    topic_id: TopicId,
    expected_events: usize,
) -> TestResult<()> {
    for _ in 0..12 {
        let _ = sync(a_dev, a, b, topic_id).await;
        let _ = sync(b_dev, b, a, topic_id).await;
        if history_texts(a_dev, a, topic_id).await?.len() == expected_events
            && history_texts(b_dev, b, topic_id).await?.len() == expected_events
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(io_error("pair did not converge").into())
}

pub async fn converge_all(
    env: &PatchLab,
    topic_id: TopicId,
    nodes: &[&PatchNode],
    expected_events: usize,
) -> TestResult<()> {
    for _ in 0..16 {
        for from in nodes {
            for to in nodes {
                if from.node.peer_id() != to.node.peer_id() {
                    let dev = env.device_for(from.node.peer_id())?;
                    let _ = sync(dev, from, to, topic_id).await;
                }
            }
        }
        let mut converged = true;
        for node in nodes {
            let dev = env.device_for(node.node.peer_id())?;
            converged &= history_texts(dev, node, topic_id)
                .await
                .is_ok_and(|history| history.len() == expected_events);
        }
        if converged {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(io_error("nodes did not converge").into())
}

pub async fn history_texts(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
) -> TestResult<Vec<String>> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        Ok(node
            .open_topic::<PatchNote>(topic_id)?
            .history(HistoryOrder::OldestFirst)?
            .into_iter()
            .map(|record| record.event.text)
            .collect())
    })
    .await
}

pub async fn raw_history_len(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
) -> TestResult<usize> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        Ok(node.raw_topic(topic_id)?.history()?.len())
    })
    .await
}

pub async fn raw_heads_len(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
) -> TestResult<usize> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        Ok(node.raw_topic(topic_id)?.heads()?.len())
    })
    .await
}

pub async fn topic_missing(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
) -> TestResult<bool> {
    let node = owner.node.clone();
    in_device(dev, move || async move {
        Ok(node.storage().topic_state(&topic_id)?.is_none())
    })
    .await
}

pub async fn assert_current_members(
    dev: &::patchbay::Device,
    owner: &PatchNode,
    topic_id: TopicId,
    members: impl IntoIterator<Item = PeerId>,
) -> TestResult<()> {
    let node = owner.node.clone();
    let expected = members.into_iter().collect::<BTreeSet<_>>();
    in_device(dev, move || async move {
        let state = node
            .storage()
            .topic_state(&topic_id)?
            .ok_or_else(|| io_error("missing topic state"))?;
        assert_eq!(state.members, expected);
        Ok(())
    })
    .await
}

async fn in_device<T, F, Fut>(dev: &::patchbay::Device, f: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    dev.spawn(move |_| f())?.await?
}

fn io_error(message: &str) -> std::io::Error {
    std::io::Error::other(message.to_owned())
}
