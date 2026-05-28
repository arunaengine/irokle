// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{AutoEvent, AutoIrokle, AutoProjection};
use irokle::history::HistoryOrder;
use irokle::reducer::EventRecord;
use irokle::{Irokle, MemoryStorage, Storage, Topic, TopicConfig, TopicId};
use serde::{Deserialize, Serialize};

const SNAPSHOT_MAGIC: &[u8; 8] = b"AUTOIRK2";

#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: serde::de::DeserializeOwned"
))]
struct Snapshot<T: AutoIrokle> {
    topic_id: TopicId,
    event_type_id: String,
    projection: AutoProjection<T>,
}

#[derive(Clone)]
pub struct AutoDoc<T: AutoIrokle, S: Storage = MemoryStorage> {
    topic: Topic<AutoEvent<T>, S>,
    projection: AutoProjection<T>,
}

impl<T: AutoIrokle, S: Storage> AutoDoc<T, S> {
    pub(crate) fn new(topic: Topic<AutoEvent<T>, S>, projection: AutoProjection<T>) -> Self {
        Self { topic, projection }
    }

    pub fn id(&self) -> TopicId {
        self.topic.id()
    }

    pub fn topic(&self) -> &Topic<AutoEvent<T>, S> {
        &self.topic
    }

    pub fn state(&self) -> &T {
        self.projection
            .state()
            .expect("auto-irokle document opened without init event")
    }

    pub fn projection(&self) -> &AutoProjection<T> {
        &self.projection
    }

    #[tracing::instrument(skip_all, fields(topic_id = %self.topic.id()))]
    pub fn refresh(&mut self) -> irokle::Result<()> {
        let applied_clock = self.projection.applied_clock().clone();
        for record in self
            .topic
            .history_after(&applied_clock, HistoryOrder::OldestFirst)?
        {
            self.projection.apply(&record)?;
        }
        Ok(())
    }

    pub fn gc(&mut self) -> irokle::Result<usize> {
        let stability = self.topic.observed_clock()?;
        Ok(self.projection.gc(&stability))
    }

    pub fn snapshot(&self) -> irokle::Result<Vec<u8>> {
        self.projection.state()?;
        let snapshot = Snapshot {
            topic_id: self.topic.id(),
            event_type_id: T::EVENT_TYPE_ID.to_owned(),
            projection: self.projection.clone(),
        };
        let mut bytes = SNAPSHOT_MAGIC.to_vec();
        bytes.extend_from_slice(&postcard::to_allocvec(&snapshot)?);
        Ok(bytes)
    }

    #[tracing::instrument(skip_all, fields(topic_id = %self.topic.id(), ops = tracing::field::Empty))]
    pub fn change<F>(&mut self, change: F) -> irokle::Result<Option<EventRecord<AutoEvent<T>>>>
    where
        F: FnOnce(&mut T),
    {
        self.refresh()?;
        let old = self.projection.state()?.clone();
        let mut next = old.clone();
        change(&mut next);

        let mut ops = Vec::new();
        T::diff_into(&[], &old, &next, &mut ops)?;
        tracing::Span::current().record("ops", ops.len());
        if ops.is_empty() {
            return Ok(None);
        }

        let record = self.topic.publish(AutoEvent::patch(ops))?;
        self.projection.apply(&record)?;
        Ok(Some(record))
    }
}

pub trait AutoIrokleExt<S: Storage> {
    fn create_doc<T: AutoIrokle>(
        &self,
        initial: T,
        config: TopicConfig,
    ) -> irokle::Result<AutoDoc<T, S>>;

    fn open_doc<T: AutoIrokle>(&self, topic_id: TopicId) -> irokle::Result<AutoDoc<T, S>>;

    fn open_doc_from_snapshot<T: AutoIrokle>(
        &self,
        topic_id: TopicId,
        snapshot: &[u8],
    ) -> irokle::Result<AutoDoc<T, S>>;
}

impl<S: Storage> AutoIrokleExt<S> for Irokle<S> {
    #[tracing::instrument(skip_all)]
    fn create_doc<T: AutoIrokle>(
        &self,
        initial: T,
        config: TopicConfig,
    ) -> irokle::Result<AutoDoc<T, S>> {
        let topic = self.create_topic::<AutoEvent<T>>(config)?;
        let record = topic.publish(AutoEvent::init(&initial)?)?;
        let mut projection = AutoProjection::new();
        projection.apply(&record)?;
        Ok(AutoDoc::new(topic, projection))
    }

    #[tracing::instrument(skip_all, fields(%topic_id))]
    fn open_doc<T: AutoIrokle>(&self, topic_id: TopicId) -> irokle::Result<AutoDoc<T, S>> {
        let topic = self.open_topic::<AutoEvent<T>>(topic_id)?;
        let mut projection = AutoProjection::new();
        for record in topic.history_after(projection.applied_clock(), HistoryOrder::OldestFirst)? {
            projection.apply(&record)?;
        }
        projection.state()?;
        Ok(AutoDoc::new(topic, projection))
    }

    #[tracing::instrument(skip_all, fields(%topic_id))]
    fn open_doc_from_snapshot<T: AutoIrokle>(
        &self,
        topic_id: TopicId,
        snapshot: &[u8],
    ) -> irokle::Result<AutoDoc<T, S>> {
        if snapshot.len() < SNAPSHOT_MAGIC.len()
            || &snapshot[..SNAPSHOT_MAGIC.len()] != SNAPSHOT_MAGIC
        {
            return Err(irokle::Error::Decode(
                "auto-irokle snapshot magic mismatch".into(),
            ));
        }
        let snapshot: Snapshot<T> = postcard::from_bytes(&snapshot[SNAPSHOT_MAGIC.len()..])
            .map_err(|err| irokle::Error::Decode(err.to_string()))?;
        if snapshot.topic_id != topic_id {
            return Err(irokle::Error::Decode(
                "auto-irokle snapshot topic mismatch".into(),
            ));
        }
        if snapshot.event_type_id != T::EVENT_TYPE_ID {
            return Err(irokle::Error::Decode(
                "auto-irokle snapshot event type mismatch".into(),
            ));
        }
        snapshot.projection.state()?;
        let topic = self.open_topic::<AutoEvent<T>>(topic_id)?;
        if !topic
            .actor_clock()?
            .dominates(snapshot.projection.applied_clock())
        {
            return Err(irokle::Error::Decode(
                "auto-irokle snapshot frontier is missing from local topic".into(),
            ));
        }
        let mut doc = AutoDoc::new(topic, snapshot.projection);
        doc.refresh()?;
        Ok(doc)
    }
}
