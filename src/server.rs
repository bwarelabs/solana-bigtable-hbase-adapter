use futures::stream::iter;
use futures::{stream, StreamExt};
use std::net::ToSocketAddrs;
use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

#[allow(clippy::derive_partial_eq_without_eq, clippy::enum_variant_names)]
mod google {
    pub(crate) mod rpc {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            concat!("/proto/google.rpc.rs")
        ));
    }
    pub mod bigtable {
        pub mod v2 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                concat!("/proto/google.bigtable.v2.rs")
            ));
        }
    }
}
use google::bigtable::v2::*;
use google::rpc::Status as RpcStatus;

use crate::google::bigtable::v2::read_rows_response::{cell_chunk, CellChunk};
use crate::google::bigtable::v2::row_range::{EndKey, StartKey};
use {
    hbase_thrift::hbase::{BatchMutation, HbaseSyncClient, THbaseSyncClient, TScan},
    hbase_thrift::MutationBuilder,
    log::*,
    std::collections::BTreeMap,
    std::convert::TryInto,
    std::time::Duration,
    thiserror::Error,
    thrift::{
        protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
        transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel},
    },
};

pub type RowKey = String;
pub type RowData = Vec<(CellName, Timestamp, CellValue)>;
pub type RowDataSlice<'a> = &'a [(CellName, CellValue)];
pub type CellName = String;
pub type CellValue = Vec<u8>;
pub type Timestamp = i64;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(std::io::Error),

    #[error("Row not found")]
    RowNotFound,

    #[error("Row write failed")]
    RowWriteFailed,

    #[error("Row delete failed")]
    RowDeleteFailed,

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Object is corrupt: {0}")]
    ObjectCorrupt(String),

    #[error("Timeout")]
    Timeout,

    #[error("Thrift")]
    Thrift(thrift::Error),
}

impl std::convert::From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl std::convert::From<thrift::Error> for Error {
    fn from(err: thrift::Error) -> Self {
        Self::Thrift(err)
    }
}

#[derive(Clone)]
pub struct HBaseConnection {
    address: String,
    // timeout: Option<Duration>,
}

impl HBaseConnection {
    pub async fn new(
        address: &str,
        _read_only: bool,
        _timeout: Option<Duration>,
    ) -> Result<Self, Error> {
        println!("Creating HBase connection instance");

        Ok(Self {
            address: address.to_string(),
            // timeout,
        })
    }

    pub fn client(&self) -> HBase {
        let mut channel = TTcpChannel::new();

        channel.open(self.address.clone()).unwrap();

        let (input_chan, output_chan) = channel.split().unwrap();

        let input_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(input_chan), true);
        let output_prot =
            TBinaryOutputProtocol::new(TBufferedWriteTransport::new(output_chan), true);

        let client = HbaseSyncClient::new(input_prot, output_prot);

        HBase { client }
    }
}

type InputProtocol =
    TBinaryInputProtocol<TBufferedReadTransport<thrift::transport::ReadHalf<TTcpChannel>>>;
type OutputProtocol =
    TBinaryOutputProtocol<TBufferedWriteTransport<thrift::transport::WriteHalf<TTcpChannel>>>;

pub struct HBase {
    client: HbaseSyncClient<InputProtocol, OutputProtocol>,
    // timeout: Option<Duration>,
}

impl HBase {
    /// Get latest data from `table`.
    ///
    /// All column families are accepted, and only the latest version of each column cell will be
    /// returned.
    ///
    /// If `start_at` is provided, the row key listing will start with key, or the next key in the
    /// table if the explicit key does not exist. Otherwise the listing will start from the start
    /// of the table.
    ///
    /// If `end_at` is provided, the row key listing will end at the key. Otherwise it will
    /// continue until the `rows_limit` is reached or the end of the table, whichever comes first.
    /// If `rows_limit` is zero, this method will return an empty array.
    pub async fn get_row_data(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        rows_limit: i64,
    ) -> Result<Vec<(RowKey, RowData)>, Error> {
        if rows_limit == 0 {
            // todo, ReadRowsRequest, default 0 should be treated as no limit
            return Ok(vec![]);
        }

        println!(
            "Trying to get rows from table {:?} in range {:?} - {:?} with limit {:?}",
            table_name, start_at, end_at, rows_limit
        );

        // solana lite rpc doesn't know about 'projects' and 'instances' so i am removing them in hbase
        let table_name = table_name.split('/').last().unwrap();

        let scan = TScan {
            start_row: Option::from(start_at.unwrap_or_default().into_bytes()),
            stop_row: Option::from(end_at.unwrap_or_default().into_bytes()),
            columns: None,
            timestamp: None,
            caching: Some(100),
            batch_size: Some(10),
            filter_string: None,
            ..Default::default()
        };

        let table_name = table_name.as_bytes().to_vec();
        let scan_id = self
            .client
            .scanner_open_with_scan(table_name, scan, BTreeMap::new())?;

        let mut results: Vec<(RowKey, RowData)> = Vec::new();
        let mut count = 0;

        loop {
            let row_results = self.client.scanner_get_list(scan_id, rows_limit as i32)?;

            if row_results.is_empty() {
                break;
            }

            for row_result in row_results {
                let row_key_bytes = row_result.row.unwrap();
                let row_key = String::from_utf8(row_key_bytes.clone()).unwrap();
                let mut column_values: RowData = Vec::new();
                for (key, column) in row_result.columns.unwrap_or_default() {
                    let column_value_bytes = column.value.unwrap_or_default();
                    let timestamp_micros = column.timestamp.unwrap_or_default() * 1000; // Convert milliseconds to microseconds if needed
                    column_values.push((
                        String::from_utf8(key).unwrap(),
                        timestamp_micros,
                        column_value_bytes,
                    ));
                }
                results.push((row_key, column_values));
                count += 1;
                if count >= rows_limit {
                    break;
                }
            }
            if count >= rows_limit {
                break;
            }
        }

        self.client.scanner_close(scan_id)?;

        Ok(results)
    }

    async fn put_row_data(
        &mut self,
        table_name: &str,
        family_name: &str,
        row_data: &Vec<(RowKey, RowData)>,
    ) -> Result<(), Error> {
        let mut mutation_batches = Vec::new();

        // solana lite rpc doesn't know about 'projects' and 'instances' so i am removing them in hbase
        let table_name = table_name.split('/').last().unwrap();

        for (row_key, cell_data) in row_data {
            let mut mutations = Vec::new();
            for (cell_name, _timestamp, cell_value) in cell_data {
                let mut mutation_builder = MutationBuilder::default();

                if cell_name == "delete_from_row" {
                    self.client.delete_all_row(
                        table_name.as_bytes().to_vec(),
                        row_key.as_bytes().to_vec(),
                        BTreeMap::new(),
                    )?;
                } else if cell_value.is_empty() {
                    if cell_name.ends_with(":*") {
                        // Handle delete from family
                        let family = cell_name.split(':').next().unwrap_or("");

                        let column_name = format!("{}:proto", family_name);

                        self.client.delete_all(
                            table_name.as_bytes().to_vec(),
                            row_key.as_bytes().to_vec(),
                            column_name.as_bytes().to_vec(),
                            BTreeMap::new(),
                        )?;
                    } else {
                        let parts: Vec<&str> = cell_name.split(':').collect();
                        if parts.len() == 2 {
                            self.client.delete_all(
                                table_name.as_bytes().to_vec(),
                                row_key.as_bytes().to_vec(),
                                cell_name.as_bytes().to_vec(),
                                BTreeMap::new(),
                            )?;
                        }
                    }
                } else {
                    mutation_builder
                        .column(family_name, cell_name)
                        .value(cell_value.clone());
                    mutations.push(mutation_builder.build());
                }
            }
            if !mutations.is_empty() {
                mutation_batches.push(BatchMutation::new(
                    Some(row_key.as_bytes().to_vec()),
                    Some(mutations),
                ));
            }
        }

        self.client.mutate_rows(
            table_name.as_bytes().to_vec(),
            mutation_batches,
            Default::default(),
        )?;

        Ok(())
    }
}

#[derive(Default)]
pub struct MyBigtableServer {}

fn start_key_to_bytes(start_key: StartKey) -> Vec<u8> {
    match start_key {
        StartKey::StartKeyClosed(bytes) => bytes,
        StartKey::StartKeyOpen(bytes) => bytes,
    }
}

fn end_key_to_bytes(end_key: EndKey) -> Vec<u8> {
    match end_key {
        EndKey::EndKeyClosed(bytes) => bytes,
        EndKey::EndKeyOpen(bytes) => bytes,
    }
}

fn value_to_string(value: Value) -> Result<String, Status> {
    match value.kind {
        Some(value::Kind::RawValue(bytes)) => String::from_utf8(bytes)
            .map_err(|e| Status::internal(format!("Failed to convert bytes to string: {}", e))),
        Some(value::Kind::IntValue(int_val)) => {
            Ok(int_val.to_string()) // Convert integer value to string
        }
        Some(value::Kind::RawTimestampMicros(timestamp)) => {
            Ok(timestamp.to_string()) // Convert timestamp to string
        }
        None => Err(Status::internal("No value set")),
    }
}

fn value_to_bytes(value: Value) -> Result<Vec<u8>, Status> {
    match value.kind {
        Some(value::Kind::RawValue(bytes)) => Ok(bytes),
        Some(value::Kind::IntValue(int_val)) => Ok(int_val.to_be_bytes().to_vec()), // Convert integer to big-endian bytes
        Some(value::Kind::RawTimestampMicros(timestamp)) => Ok(timestamp.to_be_bytes().to_vec()), // Convert timestamp to big-endian bytes
        None => Err(Status::internal("No value set")),
    }
}

fn value_to_i64(value: Value) -> Result<i64, Status> {
    match value.kind {
        Some(value::Kind::RawValue(bytes)) => {
            if bytes.len() == 8 {
                let arr: [u8; 8] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| Status::internal("Failed to convert bytes to i64"))?;
                Ok(i64::from_be_bytes(arr))
            } else {
                Err(Status::internal(
                    "RawValue does not have the correct length to be converted to i64",
                ))
            }
        }
        Some(value::Kind::IntValue(int_val)) => Ok(int_val),
        Some(value::Kind::RawTimestampMicros(timestamp)) => Ok(timestamp),
        None => Err(Status::internal("No value set")),
    }
}

fn success_status() -> Status {
    Status::new(tonic::Code::Ok, "Success")
}

fn error_status(message: &str) -> Status {
    Status::new(tonic::Code::Cancelled, message)
}

#[tonic::async_trait]
impl bigtable_server::Bigtable for MyBigtableServer {
    type ReadRowsStream = Pin<Box<dyn Stream<Item = Result<ReadRowsResponse, Status>> + Send>>;
    async fn read_rows(
        &self,
        request: Request<ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        println!("read_rows");
        let connection = HBaseConnection::new("localhost:9090", false, None)
            .await
            .expect("ok");
        let mut hbase = connection.client();
        let r = request.into_inner();

        let mut start_at: Option<RowKey> = None;
        let mut end_at: Option<RowKey> = None;

        // this code is not production ready, it is in the early stages of development where I
        // am trying to make it work
        if !r.rows.is_none() {
            let start_key = r.rows.to_owned().unwrap().row_ranges[0]
                .to_owned()
                .start_key;
            let start_key_as_bytes = start_key_to_bytes(start_key.unwrap());
            start_at = String::from_utf8(start_key_as_bytes).ok();

            let end_key = r.rows.unwrap().row_ranges[0].to_owned().end_key;
            if end_key.is_some() {
                let end_key_as_bytes = end_key_to_bytes(end_key.unwrap());
                end_at = String::from_utf8(end_key_as_bytes).ok();
            }
        }

        let hbase_data: Vec<(RowKey, RowData)> = hbase
            .get_row_data(&r.table_name, start_at, end_at, r.rows_limit)
            .await
            .unwrap();

        let response_stream = stream::iter(hbase_data.into_iter().map(|(row_key, cells)| {
            let mut chunks = Vec::new();
            let mut is_new_row = true;
            for (cell_name, timestamp, cell_value) in cells {
                chunks.push(CellChunk {
                    row_key: row_key.clone().into_bytes(),
                    family_name: Option::from("x".to_string()),
                    qualifier: Option::from(cell_name.into_bytes()),
                    value: cell_value,
                    timestamp_micros: timestamp,
                    labels: Vec::new(),
                    value_size: 0,
                    row_status: if is_new_row {
                        Option::from(cell_chunk::RowStatus::ResetRow(true))
                    } else {
                        Option::from(cell_chunk::RowStatus::CommitRow(true))
                    },
                });
                is_new_row = false;
            }
            Ok(ReadRowsResponse {
                chunks,
                last_scanned_row_key: row_key.into_bytes(),
                request_stats: None,
            })
        }))
        .boxed();

        Ok(Response::new(response_stream))
    }

    type MutateRowsStream = Pin<Box<dyn Stream<Item = Result<MutateRowsResponse, Status>> + Send>>;
    async fn mutate_rows(
        &self,
        request: Request<MutateRowsRequest>,
    ) -> Result<Response<Self::MutateRowsStream>, Status> {
        let connection = match HBaseConnection::new("localhost:9090", false, None).await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("Failed to connect to HBase: {:?}", e);
                return Err(Status::internal("Failed to connect to HBase"));
            }
        };

        let mut hbase = connection.client();
        let r = request.into_inner();
        let mut response_entries: Vec<mutate_rows_response::Entry> = Vec::new();

        for (index, entry) in r.entries.into_iter().enumerate() {
            match self.generate_and_save_entries(&mut hbase, &r.table_name, entry, index as i64).await {
                Ok(status) => response_entries.push(status),
                Err(e) => {
                    eprintln!("Error processing entry {}: {:?}", index, e);
                    response_entries.push(self.create_error_entry(index, &e.to_string()));
                }
            }
        }

        let response = MutateRowsResponse {
            entries: response_entries,
            rate_limit_info: None,
        };

        let stream = iter(vec![Ok(response)]);

        Ok(Response::new(Box::pin(stream) as Self::MutateRowsStream))
    }
}

impl MyBigtableServer {
    async fn generate_and_save_entries(
        &self,
        hbase: &mut HBase,
        table_name: &str,
        entry: mutate_rows_request::Entry,
        index: i64
    ) -> Result<mutate_rows_response::Entry, Box<dyn std::error::Error>> {
        let row_key = String::from_utf8(entry.row_key.clone())
            .map_err(|e| format!("Failed to convert row key to string: {}", e))?;

        let mut row_data_entries: RowData = vec![];

        for mutation in entry.mutations {
            match mutation.mutation {
                Some(mutation::Mutation::SetCell(set_cell)) => {
                    let cell_name = String::from_utf8(set_cell.column_qualifier).map_err(|e| {
                        format!("Failed to convert column qualifier to string: {}", e)
                    })?;
                    row_data_entries.push((cell_name, set_cell.timestamp_micros, set_cell.value));
                }
                Some(mutation::Mutation::AddToCell(add_to_cell)) => {
                    let column_qualifier = add_to_cell.column_qualifier.unwrap_or_default();
                    let cell_name = format!(
                        "{}:{}",
                        add_to_cell.family_name,
                        value_to_string(column_qualifier)?
                    );
                    let timestamp = value_to_i64(add_to_cell.timestamp.unwrap_or_default())?;
                    let cell_value = value_to_bytes(add_to_cell.input.unwrap())?;
                    row_data_entries.push((cell_name, timestamp, cell_value));
                }
                Some(mutation::Mutation::DeleteFromColumn(delete_from_column)) => {
                    let cell_name = format!(
                        "{}:{}",
                        delete_from_column.family_name,
                        String::from_utf8(delete_from_column.column_qualifier).map_err(
                            |e| format!("Failed to convert column qualifier to string: {}", e)
                        )?
                    );
                    row_data_entries.push((cell_name, 0, vec![]));
                }
                Some(mutation::Mutation::DeleteFromFamily(delete_from_family)) => {
                    let cell_name = format!("{}:*", delete_from_family.family_name);
                    row_data_entries.push((cell_name, 0, vec![]));
                }
                Some(mutation::Mutation::DeleteFromRow(_)) => {
                    row_data_entries.push(("delete_from_row".to_string(), 0, vec![]));
                }
                None => {}
            }
        }

        hbase
            .put_row_data(
                table_name,
                "x",
                &vec![(row_key.clone(), row_data_entries.clone())],
            )
            .await
            .map_err(|e| format!("HBase error: {}", e))?;

        Ok(self.create_success_entry(index))
    }

    fn create_success_entry(
        &self,
        index: i64) -> mutate_rows_response::Entry {
        mutate_rows_response::Entry {
            index,
            status: Some(RpcStatus {
                code: 0, // Replace with the actual success code
                message: "Success".to_string(),
                details: vec![],
            }),
        }
    }

    fn create_error_entry(
        &self,
        index: usize, message: &str) -> mutate_rows_response::Entry {
        mutate_rows_response::Entry {
            index: index as i64,
            status: Some(RpcStatus {
                code: 13, // gRPC INTERNAL error code
                message: message.to_string(),
                details: vec![],
            }),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = MyBigtableServer {};
    Server::builder()
        .add_service(bigtable_server::BigtableServer::new(server))
        .serve("127.0.0.1:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();
    Ok(())
}
