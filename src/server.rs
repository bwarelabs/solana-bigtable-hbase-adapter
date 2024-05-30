use futures::{stream, StreamExt};
use prost::Message;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

#[allow(clippy::derive_partial_eq_without_eq, clippy::enum_variant_names)]
mod google {
    mod rpc {
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

use crate::google::bigtable::v2::read_rows_response::{cell_chunk, CellChunk};
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
use crate::google::bigtable::v2::row_range::{EndKey, StartKey};

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
    /// Get `table` row keys in lexical order.
    ///
    /// If `start_at` is provided, the row key listing will start with key.
    /// Otherwise the listing will start from the start of the table.
    ///
    /// If `end_at` is provided, the row key listing will end at the key. Otherwise it will
    /// continue until the `rows_limit` is reached or the end of the table, whichever comes first.
    /// If `rows_limit` is zero, this method will return an empty array.
    pub async fn get_row_keys(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        rows_limit: i64,
    ) -> Result<Vec<RowKey>, Error> {
        if rows_limit == 0 {
            return Ok(vec![]);
        }

        println!(
            "Trying to get row keys in range {:?} - {:?} with limit {:?}",
            start_at, end_at, rows_limit
        );

        let mut scan = TScan::default();
        scan.start_row = start_at.map(|start_key| start_key.into_bytes());
        scan.stop_row = end_at.map(|end_key| end_key.into_bytes());
        scan.columns = None;
        scan.batch_size = Some(rows_limit as i32);
        scan.timestamp = None;
        scan.caching = rows_limit.try_into().ok();

        let scan_id = self.client.scanner_open_with_scan(
            table_name.as_bytes().to_vec(),
            scan,
            BTreeMap::new(),
        )?;

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
                    column_values.push((
                        String::from_utf8(key).unwrap(),
                        column.timestamp.unwrap_or_default() * 1000,
                        column_value_bytes.into(),
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

        Ok(results.into_iter().map(|r| r.0).collect())
    }

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
            return Ok(vec![]);
        }

        println!(
            "Trying to get rows from table {:?} in range {:?} - {:?} with limit {:?}",
            table_name, start_at, end_at, rows_limit
        );


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
                    // column_values.push((String::from_utf8(key).unwrap(), column_value_bytes.into()));
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

    pub async fn get_single_row_data(
        &mut self,
        table_name: &str,
        row_key: RowKey,
    ) -> Result<RowData, Error> {
        println!(
            "Trying to get row data with key {:?} from table {:?}",
            row_key, table_name
        );

        let row_result = self
            .client
            .get_row_with_columns(
                table_name.as_bytes().to_vec(),
                row_key.as_bytes().to_vec(),
                vec![b"x".to_vec()],
                BTreeMap::new(),
            )
            .unwrap_or_else(|err| {
                println!("get_row_with_columns error: {}", err);
                std::process::exit(1);
            });

        let first_row_result = &row_result.into_iter().next().ok_or(Error::RowNotFound)?;

        let mut result_value: RowData = vec![];
        if let Some(cols) = &first_row_result.columns {
            for (col_name, cell) in cols {
                if let Some(value) = &cell.value {
                    result_value.push((
                        String::from_utf8(col_name.to_vec()).unwrap().to_string(),
                        cell.timestamp.unwrap_or_default() * 1000,
                        value.to_vec(),
                    ));
                }
            }
        }

        Ok(result_value)
    }

    async fn put_row_data(
        &mut self,
        table_name: &str,
        family_name: &str,
        row_data: &[(&RowKey, RowData)],
    ) -> Result<(), Error> {
        let mut mutation_batches = Vec::new();
        for (row_key, cell_data) in row_data {
            let mut mutations = Vec::new();
            for (cell_name, _timestamp, cell_value) in cell_data {
                let mut mutation_builder = MutationBuilder::default();
                mutation_builder.column(family_name, cell_name);
                mutation_builder.value(cell_value.clone());
                mutations.push(mutation_builder.build());
            }
            mutation_batches.push(BatchMutation::new(
                Some(row_key.as_bytes().to_vec()),
                mutations,
            ));
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
        if !r.rows.is_none() {
            let start_key = r.rows.to_owned().unwrap().row_ranges[0].to_owned().start_key;
            let start_key_as_bytes = start_key_to_bytes(start_key.unwrap());
            start_at = String::from_utf8(start_key_as_bytes).ok();

            let end_key = r.rows.unwrap().row_ranges[0].to_owned().end_key;
            let end_key_as_bytes = end_key_to_bytes(end_key.unwrap());
            end_at = String::from_utf8(end_key_as_bytes).ok();
        }


        let hbase_data: Vec<(RowKey, RowData)> = hbase
            .get_row_data(&r.table_name,
                          // None,
                          start_at,
                          // r.rows.unwrap().row_ranges[0].end_key
                            end_at,
                          r.rows_limit)
            .await
            .unwrap();

        // dbg!(hbase_data);

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

        // Err(Status::unimplemented("not implemented"))
    }
    type MutateRowsStream = Pin<Box<dyn Stream<Item = Result<MutateRowsResponse, Status>> + Send>>;
    async fn mutate_rows(
        &self,
        _: Request<MutateRowsRequest>,
    ) -> Result<Response<Self::MutateRowsStream>, Status> {
        Err(Status::unimplemented("not implemented"))
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
