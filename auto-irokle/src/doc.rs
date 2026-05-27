// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{AutoEvent, AutoIrokle, AutoProjection};
use irokle::history::HistoryOrder;
use irokle::reducer::EventRecord;
use irokle::{Irokle, MemoryStorage, Storage, Topic, TopicConfig, TopicId};

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

    pub fn refresh(&mut self) -> irokle::Result<()> {
        self.projection = rebuild_projection(&self.topic)?;
        Ok(())
    }

    pub fn change<F>(&mut self, change: F) -> irokle::Result<Option<EventRecord<AutoEvent<T>>>>
    where
        F: FnOnce(&mut T),
    {
        let old = self.projection.state()?.clone();
        let mut next = old.clone();
        change(&mut next);

        let ops = T::diff(&old, &next)?;
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
}

impl<S: Storage> AutoIrokleExt<S> for Irokle<S> {
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

    fn open_doc<T: AutoIrokle>(&self, topic_id: TopicId) -> irokle::Result<AutoDoc<T, S>> {
        let topic = self.open_topic::<AutoEvent<T>>(topic_id)?;
        let projection = rebuild_projection(&topic)?;
        projection.state()?;
        Ok(AutoDoc::new(topic, projection))
    }
}

fn rebuild_projection<T: AutoIrokle, S: Storage>(
    topic: &Topic<AutoEvent<T>, S>,
) -> irokle::Result<AutoProjection<T>> {
    let mut projection = AutoProjection::new();
    for record in topic.history(HistoryOrder::OldestFirst)? {
        projection.apply(&record)?;
    }
    Ok(projection)
}
