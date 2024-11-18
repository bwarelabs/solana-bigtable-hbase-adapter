mod config;

use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult};
use futures::stream::iter;
use futures::{stream, StreamExt};
use hbase_thrift::hbase::TRowResult;
use lazy_static::lazy_static;
use serde_json::to_string_pretty;
use std::env;
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

use crate::config::Config;
use crate::google::bigtable::v2::read_rows_response::{cell_chunk, CellChunk};
use crate::google::bigtable::v2::row_range::{EndKey, StartKey};
use {
    hbase_thrift::hbase::{BatchMutation, HbaseSyncClient, THbaseSyncClient, TScan},
    hbase_thrift::MutationBuilder,
    log::*,
    std::collections::BTreeMap,
    std::convert::TryInto,
    thiserror::Error,
    thrift::{
        protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
        transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel},
    },
};

pub type RowKey = String;
pub type RowData = Vec<(CellName, CellValue)>;
pub type RowDataSlice<'a> = &'a [(CellName, CellValue)];
pub type CellName = String;
pub type CellValue = Vec<u8>;
pub type Timestamp = i64;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

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

    #[error("Thrift: {0}")]
    Thrift(#[from] thrift::Error),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

#[derive(Clone)]
pub struct HBaseConnection {
    address: String,
}

impl HBaseConnection {
    pub fn new(address: &str) -> Result<Self, Error> {
        info!("Creating HBase connection instance");
        Ok(Self {
            address: address.to_string(),
        })
    }

    pub fn client(&self) -> Result<HBase, Error> {
        let mut channel = TTcpChannel::new();
        channel.open(self.address.clone())?;

        let (input_chan, output_chan) = channel.split()?;

        let input_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(input_chan), true);
        let output_prot =
            TBinaryOutputProtocol::new(TBufferedWriteTransport::new(output_chan), true);

        let client = HbaseSyncClient::new(input_prot, output_prot);

        Ok(HBase { client })
    }
}

type InputProtocol =
    TBinaryInputProtocol<TBufferedReadTransport<thrift::transport::ReadHalf<TTcpChannel>>>;
type OutputProtocol =
    TBinaryOutputProtocol<TBufferedWriteTransport<thrift::transport::WriteHalf<TTcpChannel>>>;

pub struct HBase {
    client: HbaseSyncClient<InputProtocol, OutputProtocol>,
}

impl HBase {
    pub fn get_row_data(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        rows_limit: i64,
    ) -> Result<Vec<(RowKey, RowData)>, Error> {
        if rows_limit == 0 {
            return Ok(vec![]);
        }

        let table_name = table_name.split('/').last().ok_or_else(|| {
            error!(
                "Failed to split and get the last part of table name: {}",
                table_name
            );
            Error::ObjectCorrupt(table_name.to_string())
        })?;

        let start_row = start_at.map_or_else(
            || {
                error!("Start key is missing");
                Err(Error::ObjectCorrupt("Start key is missing".to_string()))
            },
            |key| Ok(key.into_bytes()),
        )?;

        let stop_row = end_at.unwrap_or_default();

        let scan = TScan {
            start_row: Some(start_row),
            stop_row: Some(stop_row.into_bytes()),
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

        let results = self.fetch_rows(scan_id, rows_limit)?;

        self.client.scanner_close(scan_id)?;

        Ok(results)
    }

    pub fn get_single_row_data(
        &mut self,
        table_name: &str,
        row_key: RowKey,
    ) -> Result<RowData, Error> {
        let table_name = table_name.split('/').last().ok_or_else(|| {
            error!(
                "Failed to split and get the last part of table name: {}",
                table_name
            );
            Error::ObjectCorrupt(table_name.to_string())
        })?;

        let row_key_bytes = row_key.clone().into_bytes();

        let scan = TScan {
            start_row: Some(row_key_bytes.clone()),
            stop_row: Some(row_key_bytes.clone()),
            columns: None,
            timestamp: None,
            caching: Some(1),
            batch_size: Some(1),
            filter_string: None,
            ..Default::default()
        };

        let table_name_bytes = table_name.as_bytes().to_vec();
        let scan_id =
            self.client
                .scanner_open_with_scan(table_name_bytes, scan, BTreeMap::new())?;

        let mut results = self.fetch_rows(scan_id, 1)?;

        self.client.scanner_close(scan_id)?;

        // Return the first row if it exists, or an error if no rows are found
        results.pop().map(|(_, row_data)| row_data).ok_or_else(|| {
            error!(
                "Row with key {:?} not found in table {}",
                row_key, table_name
            );
            Error::RowNotFound
        })
    }

    fn fetch_rows(
        &mut self,
        scan_id: i32,
        rows_limit: i64,
    ) -> Result<Vec<(RowKey, RowData)>, Error> {
        let mut results: Vec<(RowKey, RowData)> = Vec::new();
        let mut count = 0;

        loop {
            let row_results = self
                .client
                .scanner_get_list(scan_id, rows_limit as i32)
                .map_err(|e| {
                    error!("Failed to get rows from scanner: {:?}", e);
                    if let Err(close_err) = self.client.scanner_close(scan_id) {
                        error!("Failed to close scanner after error: {:?}", close_err);
                    }
                    Error::RowNotFound
                })?;

            if row_results.is_empty() {
                break;
            }

            for row_result in row_results {
                let row_key = self.process_row_result(row_result)?;
                results.push(row_key);
                count += 1;
                if count >= rows_limit {
                    break;
                }
            }

            if count >= rows_limit {
                break;
            }
        }

        Ok(results)
    }

    fn process_row_result(&self, row_result: TRowResult) -> Result<(RowKey, RowData), Error> {
        let row_key_bytes = row_result.row.ok_or_else(|| {
            error!("Row key not found in row result");
            Error::RowNotFound
        })?;

        let row_key = String::from_utf8(row_key_bytes.clone()).map_err(|e| {
            error!("Failed to convert row key to string: {:?}", e);
            Error::Utf8(e)
        })?;

        let mut column_values: RowData = Vec::new();

        for (key, column) in row_result.columns.unwrap_or_default() {
            let column_value_bytes = column.value.ok_or_else(|| {
                error!("Column value is missing for key {:?}", key);
                Error::ObjectCorrupt("Column value is missing".to_string())
            })?;

            let timestamp_micros = column.timestamp.ok_or_else(|| {
                error!("Timestamp is missing for key {:?}", key);
                Error::ObjectCorrupt("Timestamp is missing".to_string())
            })? * 1000;

            let column_key = String::from_utf8(key).map_err(|e| {
                error!("Failed to convert column key to string: {:?}", e);
                Error::Utf8(e)
            })?;

            column_values.push((column_key, column_value_bytes));
        }

        Ok((row_key, column_values))
    }

    fn put_row_data(
        &mut self,
        table_name: &str,
        family_name: &str,
        row_data: &Vec<(RowKey, RowData)>,
    ) -> Result<(), Error> {
        // solana lite rpc doesn't know about 'projects' and 'instances' so i am removing them in hbase
        let table_name = table_name.split('/').last().unwrap();

        let mutation_batches =
            self.prepare_mutations(table_name.as_bytes().to_vec(), family_name, row_data)?;

        self.execute_mutations(table_name.as_bytes().to_vec(), mutation_batches)?;

        Ok(())
    }

    fn prepare_mutations(
        &mut self,
        table_name: Vec<u8>,
        family_name: &str,
        row_data: &Vec<(RowKey, RowData)>,
    ) -> Result<Vec<BatchMutation>, Error> {
        let mut mutation_batches = Vec::new();

        for (row_key, cell_data) in row_data {
            let mutations =
                self.create_mutations(table_name.clone(), family_name, row_key, cell_data)?;
            if !mutations.is_empty() {
                mutation_batches.push(BatchMutation::new(row_key.as_bytes().to_vec(), mutations));
            }
        }

        Ok(mutation_batches)
    }

    fn create_mutations(
        &mut self,
        table_name: Vec<u8>,
        family_name: &str,
        row_key: &RowKey,
        cell_data: &RowData,
    ) -> Result<Vec<hbase_thrift::hbase::Mutation>, Error> {
        let mut mutations = Vec::new();

        for (cell_name, cell_value) in cell_data {
            if cell_name == "delete_from_row" {
                self.client
                    .delete_all_row(
                        table_name.clone(),
                        row_key.as_bytes().to_vec(),
                        BTreeMap::new(),
                    )
                    .map_err(|e| {
                        error!("Failed to delete row: {:?}", e);
                        e
                    })?;
            } else if cell_value.is_empty() {
                self.handle_empty_cell_value(table_name.clone(), row_key, cell_name, family_name)?;
            } else {
                let mutation = MutationBuilder::default()
                    .column(family_name, cell_name)
                    .value(cell_value.clone())
                    .build();
                mutations.push(mutation);
            }
        }

        Ok(mutations)
    }

    fn handle_empty_cell_value(
        &mut self,
        table_name: Vec<u8>,
        row_key: &RowKey,
        cell_name: &str,
        family_name: &str,
    ) -> Result<(), Error> {
        if cell_name.ends_with(":*") {
            let column_name = format!("{}:proto", family_name);

            self.client
                .delete_all(
                    table_name,
                    row_key.as_bytes().to_vec(),
                    column_name.as_bytes().to_vec(),
                    BTreeMap::new(),
                )
                .map_err(|e| {
                    error!("Failed to delete all from family: {:?}", e);
                    e
                })?;
        } else {
            let parts: Vec<&str> = cell_name.split(':').collect();
            if parts.len() == 2 {
                self.client
                    .delete_all(
                        table_name,
                        row_key.as_bytes().to_vec(),
                        cell_name.as_bytes().to_vec(),
                        BTreeMap::new(),
                    )
                    .map_err(|e| {
                        error!("Failed to delete column: {:?}", e);
                        e
                    })?;
            }
        }
        Ok(())
    }

    fn execute_mutations(
        &mut self,
        table_name: Vec<u8>,
        mutation_batches: Vec<BatchMutation>,
    ) -> Result<(), Error> {
        self.client
            .mutate_rows(table_name, mutation_batches, Default::default())
            .map_err(|e| {
                error!("Failed to execute mutations: {:?}", e);
                Error::RowWriteFailed
            })
    }
}

pub struct MyBigtableServer {
    pool: Pool<HBaseManager>,
}

impl MyBigtableServer {
    pub fn new(pool: Pool<HBaseManager>) -> Self {
        MyBigtableServer { pool }
    }
}

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
        Some(value::Kind::IntValue(int_val)) => Ok(int_val.to_string()),
        Some(value::Kind::RawTimestampMicros(timestamp)) => Ok(timestamp.to_string()),
        None => Err(Status::internal("No value set")),
    }
}

fn value_to_bytes(value: Value) -> Result<Vec<u8>, Status> {
    match value.kind {
        Some(value::Kind::RawValue(bytes)) => Ok(bytes),
        Some(value::Kind::IntValue(int_val)) => Ok(int_val.to_be_bytes().to_vec()),
        Some(value::Kind::RawTimestampMicros(timestamp)) => Ok(timestamp.to_be_bytes().to_vec()),
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

#[tonic::async_trait]
impl bigtable_server::Bigtable for MyBigtableServer {
    type ReadRowsStream =
        Pin<Box<dyn Stream<Item = Result<ReadRowsResponse, Status>> + Send + 'static>>;

    async fn read_rows(
        &self,
        request: Request<ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        info!("Received read_rows request");

        let connection: Object<HBaseManager> = self.get_connection().await?;

        let r = request.into_inner();
        let (start_at, end_at) = self.parse_row_ranges(&r).map_err(|e| {
            error!("Failed to parse row ranges: {:?}", e);
            Status::internal("Failed to parse row ranges")
        })?;

        // info!("start_at: {:?}, end_at: {:?}", start_at, end_at);
        // info!(
        //     "table_name: {:?}, rows_limit: {:?}",
        //     r.table_name, r.rows_limit
        // );

        let table_name = r.table_name.clone();
        let rows_limit = r.rows_limit;
        let start_at_clone = start_at.clone();

        let hbase_data = tokio::task::spawn_blocking(move || {
            let mut hbase_client = connection.client().map_err(|e| {
                error!("Failed to create HBase client: {:?}", e);
                Status::internal("Failed to create HBase client")
            })?;

            hbase_client
                .get_row_data(&table_name, start_at, end_at, rows_limit)
                .map_err(|e| {
                    error!("Failed to get row data from HBase: {:?}", e);
                    Status::internal("Failed to get row data from HBase")
                })

            // hbase_client
            //     .get_single_row_data(&table_name, start_at.clone().unwrap())
            //     .map_err(|e| {
            //         error!("Failed to get row data from HBase: {:?}", e);
            //         Status::internal("Failed to get row data from HBase")
            //     })
        })
        .await
        .map_err(|e| {
            error!("Failed to execute blocking task: {:?}", e);
            Status::internal("Failed to execute blocking task")
        })??;

        // let hbase_data_copy = hbase_data.clone();

        // match to_string_pretty(&hbase_data) {
        //     Ok(json) => info!("HBase data: {}", json),
        //     Err(e) => error!("Failed to serialize HBase data: {:?}", e),
        // }

        let response_stream = stream::iter(hbase_data.into_iter().map(|(row_key, cells)| {
            Ok(ReadRowsResponse {
                chunks: MyBigtableServer::create_chunks(row_key.clone(), cells),
                last_scanned_row_key: row_key.into_bytes(),
                request_stats: None,
            })
        }))
        .boxed();

        // let response_stream = stream::once(async move {
        //     Ok(ReadRowsResponse {
        //         chunks: MyBigtableServer::create_chunks(
        //             start_at_clone.clone().unwrap(),
        //             hbase_data,
        //         ),
        //         last_scanned_row_key: start_at_clone.unwrap().into_bytes(),
        //         request_stats: None,
        //     })
        // })
        // .boxed();

        Ok(Response::new(response_stream))
    }

    type MutateRowsStream = Pin<Box<dyn Stream<Item = Result<MutateRowsResponse, Status>> + Send>>;
    async fn mutate_rows(
        &self,
        request: Request<MutateRowsRequest>,
    ) -> Result<Response<Self::MutateRowsStream>, Status> {
        info!("Received mutate_rows request {request:?}");

        let connection = self.get_connection().await?;

        let r = request.into_inner();
        let table_name = r.table_name.clone();
        let entries = r.entries.clone();

        let response_entries = tokio::task::spawn_blocking(
            move || -> Result<Vec<mutate_rows_response::Entry>, Status> {
                let mut hbase_client = connection.client().map_err(|e| {
                    error!("Failed to create HBase client: {:?}", e);
                    Status::internal("Failed to create HBase client")
                })?;
                let mut response_entries: Vec<mutate_rows_response::Entry> = Vec::new();

                for (index, entry) in entries.into_iter().enumerate() {
                    match MyBigtableServer::generate_and_save_entries(
                        &mut hbase_client,
                        table_name.as_str(),
                        entry,
                        index as i64,
                    ) {
                        Ok(status) => response_entries.push(status),
                        Err(e) => {
                            error!("Error processing entry {}: {:?}", index, e);
                            response_entries
                                .push(MyBigtableServer::create_error_entry(index, &e.to_string()));
                        }
                    }
                }

                Ok(response_entries)
            },
        )
        .await
        .map_err(|e| {
            error!("Failed to process entries: {:?}", e);
            Status::internal("Failed to process entries")
        })??;

        let response = MutateRowsResponse {
            entries: response_entries,
            rate_limit_info: None,
        };

        let stream = iter(vec![Ok(response)]);

        Ok(Response::new(Box::pin(stream) as Self::MutateRowsStream))
    }
}

impl MyBigtableServer {
    // read_rows helper function
    fn parse_row_ranges(
        &self,
        request: &ReadRowsRequest,
    ) -> Result<(Option<RowKey>, Option<RowKey>), Box<dyn std::error::Error>> {
        let rows = request
            .rows
            .as_ref()
            .ok_or("Missing rows field in parse_row_ranges")?;

        if rows.row_ranges.is_empty() && rows.row_keys.is_empty() {
            return Err("Neither row ranges nor row keys were provided".into());
        }

        let mut start_key = None;
        let start_key_as_bytes;
        let start_at;

        let mut end_at = None;

        if !rows.row_ranges.is_empty() {
            start_key = rows.row_ranges[0].start_key.clone();
            start_key_as_bytes =
                start_key_to_bytes(start_key.ok_or("Missing start key parse_row_ranges")?);
            start_at = Some(String::from_utf8(start_key_as_bytes)?);

            end_at = rows.row_ranges[0]
                .end_key
                .clone()
                .map(|end_key| {
                    let end_key_as_bytes = end_key_to_bytes(end_key);
                    String::from_utf8(end_key_as_bytes)
                })
                .transpose()?;
        } else {
            start_at = Some(String::from_utf8(rows.row_keys[0].clone())?);
        }

        Ok((start_at, end_at))
    }

    fn create_chunks(row_key: RowKey, cells: RowData) -> Vec<CellChunk> {
        let mut chunks = Vec::new();
        let mut is_new_row = true;

        for (i, (cell_name, cell_value)) in cells.into_iter().enumerate() {
            let parts: Vec<&str> = cell_name.split(':').collect();
            let family_name = parts.get(0).unwrap_or(&"").to_string();
            let qualifier = parts.get(1).unwrap_or(&"").to_string();

            // Set row_key only at the start of a new row
            let chunk_row_key = if is_new_row {
                row_key.clone().into_bytes()
            } else {
                vec![]
            };

            let chunk = CellChunk {
                row_key: chunk_row_key,
                family_name: Some(family_name),
                qualifier: Some(qualifier.into_bytes()),
                value: cell_value,
                timestamp_micros: 0,
                labels: Vec::new(),
                value_size: 0, // Set to the length of `value` if needed for pre-allocation
                row_status: if is_new_row {
                    Some(cell_chunk::RowStatus::ResetRow(true))
                } else {
                    None // No row status for intermediate cells
                },
            };

            chunks.push(chunk);
            is_new_row = false;
        }

        // Ensure the last cell in the row has CommitRow set
        if let Some(last_chunk) = chunks.last_mut() {
            last_chunk.row_status = Some(cell_chunk::RowStatus::CommitRow(true));
        }

        chunks
    }

    // mutate_rows helper function
    fn generate_and_save_entries(
        hbase: &mut HBase,
        table_name: &str,
        entry: mutate_rows_request::Entry,
        index: i64,
    ) -> Result<mutate_rows_response::Entry, Box<dyn std::error::Error>> {
        let row_key = String::from_utf8(entry.row_key.clone()).map_err(|e| {
            error!("Failed to convert row key to string: {}", e);
            Box::<dyn std::error::Error>::from(format!(
                "Failed to convert row key to string: {}",
                e
            ))
        })?;

        let mut row_data_entries: RowData = vec![];

        for mutation in entry.mutations {
            match mutation.mutation {
                Some(mutation::Mutation::SetCell(set_cell)) => {
                    let cell_name = String::from_utf8(set_cell.column_qualifier).map_err(|e| {
                        error!("Failed to convert column qualifier to string: {}", e);
                        Box::<dyn std::error::Error>::from(format!(
                            "Failed to convert column qualifier to string: {}",
                            e
                        ))
                    })?;
                    row_data_entries.push((cell_name, set_cell.value));
                }
                Some(mutation::Mutation::AddToCell(add_to_cell)) => {
                    let column_qualifier = add_to_cell.column_qualifier.ok_or_else(|| {
                        error!("Column qualifier is missing");
                        Box::<dyn std::error::Error>::from("Column qualifier is missing")
                    })?;
                    let cell_name = format!(
                        "{}:{}",
                        add_to_cell.family_name,
                        value_to_string(column_qualifier)?
                    );
                    let timestamp = value_to_i64(add_to_cell.timestamp.ok_or_else(|| {
                        error!("Timestamp is missing");
                        Box::<dyn std::error::Error>::from("Timestamp is missing")
                    })?)?;
                    let cell_value = value_to_bytes(add_to_cell.input.ok_or_else(|| {
                        error!("Input value is missing");
                        Box::<dyn std::error::Error>::from("Input value is missing")
                    })?)?;
                    row_data_entries.push((cell_name, cell_value));
                }
                Some(mutation::Mutation::DeleteFromColumn(delete_from_column)) => {
                    let cell_name = format!(
                        "{}:{}",
                        delete_from_column.family_name,
                        String::from_utf8(delete_from_column.column_qualifier).map_err(|e| {
                            error!("Failed to convert column qualifier to string: {}", e);
                            Box::<dyn std::error::Error>::from(format!(
                                "Failed to convert column qualifier to string: {}",
                                e
                            ))
                        })?
                    );
                    row_data_entries.push((cell_name, vec![]));
                }
                Some(mutation::Mutation::DeleteFromFamily(delete_from_family)) => {
                    let cell_name = format!("{}:*", delete_from_family.family_name);
                    row_data_entries.push((cell_name, vec![]));
                }
                Some(mutation::Mutation::DeleteFromRow(_)) => {
                    row_data_entries.push(("delete_from_row".to_string(), vec![]));
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
            .map_err(|e| {
                error!("HBase error: {}", e);
                Box::<dyn std::error::Error>::from(format!("HBase error: {}", e))
            })?;

        Ok(MyBigtableServer::create_success_entry(index))
    }

    fn create_success_entry(index: i64) -> mutate_rows_response::Entry {
        mutate_rows_response::Entry {
            index,
            status: Some(RpcStatus {
                code: 0,
                message: "Success".to_string(),
                details: vec![],
            }),
        }
    }

    fn create_error_entry(index: usize, message: &str) -> mutate_rows_response::Entry {
        mutate_rows_response::Entry {
            index: index as i64,
            status: Some(RpcStatus {
                code: 13, // internal error
                message: message.to_string(),
                details: vec![],
            }),
        }
    }

    async fn get_connection(&self) -> Result<Object<HBaseManager>, Status> {
        match self.pool.get().await {
            Ok(conn) => Ok(conn),
            Err(e) => {
                error!("Failed to get HBase connection from pool: {:?}", e);
                return Err(Status::internal("Failed to connect to HBase"));
            }
        }
    }
}

lazy_static! {
    static ref CONFIG: Config = Config::from_env();
}

pub struct HBaseManager {
    address: String,
}

impl Manager for HBaseManager {
    type Type = HBaseConnection;
    type Error = Error;

    async fn create(&self) -> Result<HBaseConnection, Self::Error> {
        HBaseConnection::new(&self.address)
    }

    async fn recycle(
        &self,
        _connection: &mut HBaseConnection,
        _metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // env::set_var("RUST_LOG", "info");
    // env_logger::init();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .init();

    let pool = Pool::builder(HBaseManager {
        address: CONFIG.hbase_host.clone(),
    })
    .max_size(16)
    .build()
    .unwrap();

    let server = MyBigtableServer::new(pool);

    info!("Starting server on 0.0.0.0:50051");
    let hbase_host = CONFIG.hbase_host.clone();
    info!("HBASE_HOST env : {hbase_host:}");

    Server::builder()
        .add_service(
            bigtable_server::BigtableServer::new(server)
                .max_decoding_message_size(128 * 1024 * 1024), // 128 MB
        )
        .serve("0.0.0.0:50051".to_socket_addrs().unwrap().next().unwrap())
        .await?;

    Ok(())
}
