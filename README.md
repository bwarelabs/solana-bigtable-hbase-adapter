# BigTable to HBase Adapter for Solana Validator

The Solana Validator uses Bigtable for long term storage. In order not to modify the validator 
code, we use an adaptor to proxy the Bigtable API to HBase using the ```BIGTABLE_EMULATOR_HOST``` 
connection mechanism.

This adaptor exposes the 2 Bigtable gRPC API methods (read_rows and mutate_rows)
used by the Solana Validator.

When a request is received (via the gRPC methods), the adaptor will translate the request to the HBase REST API (using HBase Thrift), fetch
the data from HBase, and then translate the response back to the gRPC Bigtable API.

## Usage

### build-proto

The `build-proto` directory clones and generates the gRPC Tonic server bindings for the
Bigtable API. We manually removed the irrelevant methods from the trait and only left
what is used by the node. This only needs to be run once as the results should be checked
into the folder `proto`.

#### This code is not production ready, it is in the early stages of development where we are trying to make it work

### Running the adapter

```sh
cargo run --bin server
```

The above command will start the adapter which listens on `localhost:50051` and tries to connect
to the `hbase` at `localhost:9090`.

You can then start the validator using the following steps:  
- clone the Solana test-setup repository: [bwarelabs/solana-test-setup](https://github.com/bwarelabs/solana-test-setup)
- bring up the hbase service: `docker compose up -d --build hbase` (note that it takes a while
        for the service to be healthy, you can check with `docker compose ps`)
- clone the Solana repository: [anza-xyz/agave](https://github.com/anza-xyz/agave)
- build and run the test validator
```sh
cd agave
BIGTABLE_EMULATOR_HOST=localhost:50051 \
    SOLANA_NO_HIDDEN_CLI_ARGS=1 \
    cargo run --bin solana-validator -- --enable-bigtable-ledger-upload --enable-rpc-bigtable-ledger-storage
```

The validator is run the same way, you just need to set the `BIGTABLE_EMULATOR_HOST` environment variable
to `localhost:50051` and add the `--enable-bigtable-ledger-upload`, `--enable-rpc-bigtable-ledger-storage`,
   and `--enable-rpc-transaction-history` flags to your command line and then restart the validator.
