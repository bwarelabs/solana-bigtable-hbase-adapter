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

### Running the adapter

```sh
cargo run --bin server
```

The above command will start the adapter which listens on `localhost:50051` and tries to connect
to the `hbase` at `localhost:9090`.

You can then start the validator using the following steps:  
- clone the Solana repository: [anza-xyz/agave](https://github.com/anza-xyz/agave)
- build and run the test validator
```sh
cd agave
GOOGLE_APPLICATION_CREDENTIALS=credentials.json \
    BIGTABLE_EMULATOR_HOST=localhost:50051 \
    SOLANA_NO_HIDDEN_CLI_ARGS=1 \
    cargo run --bin solana-test-validator -- --enable-big-table-ledger-upload
```
