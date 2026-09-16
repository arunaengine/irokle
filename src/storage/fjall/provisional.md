Provisional bootstrap namespaces in Fjall. Each namespace takes one slot of a fixed pool of keyspaces, which is cleared before another session reuses it.

The registry in the main keyspace owns every namespace, in four phases:

- staging: `bn<source><topic>` names the session and `bs<slot>` gives it the slot. A view reads while `bn` names its session and writes, checked in the writing transaction, while that session is not activating.
- activating: `bn` is frozen and `ba<topic>` claims the topic's one activation for the session. Copies into the active records stay hidden from every read until publication; each copy checks the claim.
- published: one transaction installs the topic, drops the claim and ends every namespace of the topic.
- clearing: `bs` names the ended session; every delete and the release of the slot check that session.
