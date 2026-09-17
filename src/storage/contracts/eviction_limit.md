A healthy consumer acknowledges each record as soon as it owns the payloads
durably, so this only bounds a store whose consumer stopped draining; the reset
that would exceed it is refused rather than discarding a payload nothing else
holds.
