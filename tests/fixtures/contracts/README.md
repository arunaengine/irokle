# Frozen public representations

These deterministic fixtures were written by Irokle 0.1.4 at reviewed commit `6be58d6b1e6c28382d723bea1abb4a1189ad74b6`, tree `2966a8494ebeaafe50e87128971d36390c039361`, using the test-only `contracts::write_contracts` harness. The production encoder was unchanged.

The baseline harness SHA256 is `ea26c86b6ad31209280f0c26e945906b42f36be351ea04159dc32d8490adb650`. Operations and configuration use postcard; the five sync messages use the public wire encoder. The fixture signer uses the fixed public test seed `[7; 32]`. These keys and records are test data.

`tests/contracts.rs::read_contracts` checks decoding, exact re-encoding, values and signatures against these bytes. Regeneration requires the reviewed production encoder and a new output directory. The current writer deliberately rejects the 0.2 package version.

| File | SHA256 |
| --- | --- |
| ack.bin | `d353c12d7f00b08467c97f0c6401cc7a3ddc2a0c2e6e27998e3ba30ea264b979` |
| configuration.bin | `14c516ccb5d14cf7d082fa037cff840d96205585f01f12bda619f32968f87f64` |
| operation.bin | `0a867f7f9731c8e27040ae26968e4328057ac2c04f5af37d092b9e5a7fba72a7` |
| page.bin | `8b217bcdc0fea282b02491962ade4a812dcbcd2c855ecc9794cf6a9cd0b0c047` |
| receipt.bin | `568790bc774bb554c5d7bbe5661717dcc4b9bd125600550651487b6d6e3e55d2` |
| request.bin | `562722eb09df3a242ebf32da39630ebef938bcb3ec05b3ebb04ebfead57fd5ac` |
| summary.bin | `2cda34bd2970e181240122c91150653c318945b8490ed9dce7928198cdfc0ecf` |
