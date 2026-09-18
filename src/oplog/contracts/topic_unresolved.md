Ids this topic references but cannot resolve: admitted ops whose own records are
incomplete, dependencies of admitted ops that are not fully stored, and the
holes buffered ops are still waiting for. An empty set means every admitted op
is locally usable, which is what lets sync certify the topic; anything else is
turned into concrete repair wants.
Holes a complete scan found are kept and checked again when asked, so a repair
stored through any facade of the store is seen without receiving it again.
