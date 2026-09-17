`SyncAll` preserves the historical fully durable behavior. `Buffer` avoids a
foreground fsync on every Irokle transaction and is useful when callers provide
their own durability boundary.
