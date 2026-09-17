The consumer calls this only once it durably owns the payloads, so a crash
before that point leaves the record for the next restart. Releasing an absent
key is not an error: acknowledgement is idempotent.
