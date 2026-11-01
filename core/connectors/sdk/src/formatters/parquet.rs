// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io::{BufRead, Cursor, Seek, SeekFrom};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_json::ReaderBuilder;
use arrow_json::reader::infer_json_schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::Error;

/// Converts newline-delimited JSON records from a seekable reader into a Parquet byte buffer.
///
/// If `schema` is not provided, the schema will be inferred from all records in the reader.
/// If `props` is not provided, default writer properties with ZSTD compression are used.
pub fn ndjson_to_parquet<R: BufRead + Seek>(
    mut reader: R,
    schema: Option<SchemaRef>,
    props: Option<WriterProperties>,
) -> Result<Vec<u8>, Error> {
    let target_schema = match schema {
        Some(s) => s,
        None => {
            let (inferred_schema, records_read) =
                infer_json_schema(&mut reader, None).map_err(|e| {
                    Error::WriteFailure(format!("Failed to infer Arrow schema from JSON: {e}"))
                })?;

            if records_read == 0 {
                return Err(Error::WriteFailure(
                    "Cannot create Parquet file: input contains 0 records".to_string(),
                ));
            }

            reader
                .seek(SeekFrom::Start(0))
                .map_err(|e| Error::WriteFailure(format!("Failed to rewind JSON reader: {e}")))?;

            Arc::new(inferred_schema)
        }
    };

    let batch_reader = ReaderBuilder::new(target_schema.clone())
        .build(reader)
        .map_err(|e| {
            Error::WriteFailure(format!("Failed to build JSON record batch reader: {e}"))
        })?;

    let writer_props = props.unwrap_or_else(|| {
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build()
    });

    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, target_schema, Some(writer_props))
        .map_err(|e| Error::WriteFailure(format!("Failed to initialize Parquet writer: {e}")))?;

    for batch in batch_reader {
        let record_batch =
            batch.map_err(|e| Error::WriteFailure(format!("Failed to read record batch: {e}")))?;
        writer.write(&record_batch).map_err(|e| {
            Error::WriteFailure(format!("Failed to write record batch to Parquet: {e}"))
        })?;
    }

    writer
        .close()
        .map_err(|e| Error::WriteFailure(format!("Failed to close Parquet writer: {e}")))?;

    Ok(output)
}

/// Converts an iterator of JSON byte slices into a Parquet byte buffer.
pub fn json_entries_to_parquet<'a, I>(
    entries: I,
    schema: Option<SchemaRef>,
    props: Option<WriterProperties>,
) -> Result<Vec<u8>, Error>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut combined = Vec::new();
    let mut count = 0;
    for entry in entries {
        combined.extend_from_slice(entry);
        combined.push(b'\n');
        count += 1;
    }

    if count == 0 {
        return Err(Error::WriteFailure(
            "Cannot create Parquet file: 0 entries provided".to_string(),
        ));
    }

    ndjson_to_parquet(Cursor::new(combined), schema, props)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn test_ndjson_to_parquet_basic() {
        let json_data = r#"{"id": 1, "name": "alice"}
{"id": 2, "name": "bob"}"#;

        let parquet_bytes =
            ndjson_to_parquet(Cursor::new(json_data.as_bytes()), None, None).unwrap();
        assert!(!parquet_bytes.is_empty());

        // Verify Parquet can be read back
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes)).unwrap();
        let mut reader = reader_builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_ndjson_to_parquet_nested_and_sparse() {
        let json_data = r#"{"offset": 1, "payload": {"foo": "bar"}}
{"offset": 2, "payload": {"baz": 42}}"#;

        let parquet_bytes =
            ndjson_to_parquet(Cursor::new(json_data.as_bytes()), None, None).unwrap();
        assert!(!parquet_bytes.is_empty());

        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes)).unwrap();
        let mut reader = reader_builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_json_entries_to_parquet() {
        let entries = vec![
            br#"{"offset": 1, "stream": "s1", "topic": "t1"}"#.as_slice(),
            br#"{"offset": 2, "stream": "s1", "topic": "t1"}"#.as_slice(),
        ];

        let parquet_bytes = json_entries_to_parquet(entries, None, None).unwrap();
        assert!(!parquet_bytes.is_empty());

        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes)).unwrap();
        let mut reader = reader_builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_json_entries_to_parquet_empty_errors() {
        let entries: Vec<&[u8]> = vec![];
        let result = json_entries_to_parquet(entries, None, None);
        assert!(result.is_err());
    }
}
