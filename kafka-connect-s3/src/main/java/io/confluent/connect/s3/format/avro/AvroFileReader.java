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

package io.confluent.connect.s3.format.avro;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import io.confluent.connect.s3.storage.S3Storage;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroFileReader
        implements io.confluent.connect.storage.format.SchemaFileReader<S3SinkConnectorConfig, String> {

    private static final Logger log = LoggerFactory.getLogger(AvroFileReader.class);

    private final AmazonS3 s3;
    private AvroData avroData;

    private static final int RETRY_ATTEMPTS = 3;
    private int retries;

    public AvroFileReader(S3Storage storage, AvroData avroData) {
        this.s3 = storage.newS3Client();
        this.avroData = avroData;
    }

    private org.apache.avro.Schema getSchema(InputStream is) throws IOException {
        DatumReader<Object> reader = new GenericDatumReader<>();
        DataFileStream<Object> streamReader = new DataFileStream<>(is, reader);
        org.apache.avro.Schema schema = streamReader.getSchema();
        streamReader.close();
        return schema;
    }

    @Override
    public Schema getSchema(S3SinkConnectorConfig conf, String key) {
        try (S3Object obj = s3.getObject(conf.getBucketName(), key)) {
            InputStream is = obj.getObjectContent();
            return avroData.toConnectSchema(getSchema(is));
        } catch (AmazonServiceException e) {
            log.warn("Unable to read schema. Attempting to retry.", e);
            if (e.isRetryable() && retries < RETRY_ATTEMPTS) {
                retries++;
                return getSchema(conf, key);
            } else {
                StringBuilder sb = new StringBuilder("Failed to read schema.");
                if (retries >= RETRY_ATTEMPTS) {
                    sb.append(" Max retry attempts reached.");
                }
                log.error(sb.toString(), e);
                throw new DataException(e);
            }
        } catch (IOException e) {
            throw new DataException(e);
        }
    }

    public boolean hasNext() {
        throw new UnsupportedOperationException();
    }

    public Object next() {
        throw new UnsupportedOperationException();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }

    public void close() {}
}