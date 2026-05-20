use std::collections::HashMap;

use irokle::history::HistoryOrder;
use irokle::reducer::{EventRecord, Reducer};
use irokle::{ActorClock, Ed25519Signer, Irokle, TopicConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct Quad {
    subject: String,
    predicate: String,
    object: String,
    graph: Option<String>,
}

impl Quad {
    fn new(
        subject: impl Into<String>,
        predicate: impl Into<String>,
        object: impl Into<String>,
    ) -> Self {
        Self {
            subject: subject.into(),
            predicate: predicate.into(),
            object: object.into(),
            graph: None,
        }
    }

    fn in_graph(mut self, graph: impl Into<String>) -> Self {
        self.graph = Some(graph.into());
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, irokle::Event, Serialize, Deserialize)]
#[irokle(type_id = "example.rdf/quad.v1")]
enum RdfEvent {
    AddQuad { quad: Quad },
    RemoveQuad { quad: Quad },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RdfEntry {
    add_clock: ActorClock,
    remove_clock: ActorClock,
}

impl RdfEntry {
    fn is_present(&self) -> bool {
        !self.remove_clock.dominates(&self.add_clock)
    }
}

type RdfState = HashMap<Quad, RdfEntry>;

#[derive(Clone, Debug, Default)]
struct RdfReducer;

impl Reducer<RdfEvent> for RdfReducer {
    type State = RdfState;
    type Error = std::convert::Infallible;

    fn apply(
        &mut self,
        state: &mut Self::State,
        record: &EventRecord<RdfEvent>,
    ) -> Result<(), Self::Error> {
        match &record.event {
            RdfEvent::AddQuad { quad } => {
                let entry = state.entry(quad.clone()).or_default();
                entry.add_clock.merge(&record.meta.observed_clock);
                entry
                    .add_clock
                    .observe(record.meta.actor_id, record.meta.actor_seq);
            }
            RdfEvent::RemoveQuad { quad } => state
                .entry(quad.clone())
                .or_default()
                .remove_clock
                .merge(&record.meta.observed_clock),
        }
        Ok(())
    }
}

fn quads(state: &RdfState) -> Vec<Quad> {
    state
        .iter()
        .filter_map(|(quad, entry)| entry.is_present().then_some(quad.clone()))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Irokle::builder()
        .with_signer(Ed25519Signer::from_bytes(&[3; 32]))
        .build()?;
    let topic = node.create_topic::<RdfEvent>(TopicConfig::default())?;
    let quad = Quad::new("note:1", "tag", "local-first").in_graph("notes");

    topic.publish(RdfEvent::AddQuad { quad: quad.clone() })?;
    topic.publish(RdfEvent::RemoveQuad { quad })?;

    let mut state = RdfState::default();
    let mut reducer = RdfReducer;
    for record in topic.history(HistoryOrder::OldestFirst)? {
        reducer.apply(&mut state, &record)?;
    }

    println!(
        "visible quads after observed remove: {}",
        quads(&state).len()
    );

    Ok(())
}
