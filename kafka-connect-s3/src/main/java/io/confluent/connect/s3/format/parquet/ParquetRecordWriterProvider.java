/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.parquet.avro.AvroParquetWriter.builder;

public class ParquetRecordWriterProvider
        implements io.confluent.connect.storage.format.RecordWriterProvider<S3SinkConnectorConfig> {
    private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
    private static final String EXTENSION = ".parquet";
    private static final org.apache.hadoop.conf.Configuration WRITER_CONF = new Configuration();
    private final AvroData avroData;

    private int blockSize;
    private int pageSize;
    private CompressionCodecName compressionCodecName;

    ParquetRecordWriterProvider(AvroData avroData) {
        this(avroData, null);
    }

    ParquetRecordWriterProvider(AvroData avroData, CompressionCodecName compressionCodecName) {
        this.avroData = avroData;
        this.blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
        this.pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
        if (compressionCodecName == null) {
            compressionCodecName = CompressionCodecName.UNCOMPRESSED;
        }
        this.compressionCodecName = compressionCodecName;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public io.confluent.connect.storage.format.RecordWriter getRecordWriter(
            final S3SinkConnectorConfig conf,
            final String filename) {
        return new RecordWriter() {
            ParquetWriter<GenericRecord> writer;

            Schema schema = null;
            org.apache.hadoop.fs.Path hdfsppath = new org.apache.hadoop.fs.Path(filename);
            org.apache.avro.Schema avroSchema = null;

            @Override
            public void write(SinkRecord record) {
                if (avroSchema == null) {
                    schema = record.valueSchema();

                    try {
                        log.info("Opening record writer for: {}", filename);
                        avroSchema = avroData.fromConnectSchema(schema);
                        writer = AvroParquetWriter
                                .<GenericRecord>builder(hdfsppath)
                                .withSchema(avroSchema)
                                .withConf(WRITER_CONF)
                                .withCompressionCodec(compressionCodecName)
                                .withPageSize(pageSize)
                                .build();
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }

                log.trace("Sink record: {}", record.toString());

                Object value = avroData.fromConnectData(record.valueSchema(), record.value());
                try {
                    writer.write((GenericRecord) value);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void close() {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
            }

            @Override
            public void commit() {
            }
        };
    }
}