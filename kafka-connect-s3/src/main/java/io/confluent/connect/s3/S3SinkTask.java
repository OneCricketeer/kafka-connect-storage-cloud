/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.s3;

import com.amazonaws.AmazonClientException;
import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.Version;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class S3SinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

  private S3SinkConnectorConfig connectorConfig;
  private String url;
  private S3Storage storage;
  private final Set<TopicPartition> assignment;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private Partitioner<FieldSchema> partitioner;
  private Format<S3SinkConnectorConfig, String> format;
  private RecordWriterProvider<S3SinkConnectorConfig> writerProvider;
  private SchemaFileReader<S3SinkConnectorConfig, String> schemaFileReader;
  private final Time time;
  private AvroData avroData;
  private String delim;
  private String topicsDir;
  private ExecutorService executorService;
  private String hiveDatabase;
  private HiveMetaStore hiveMetaStore;
  private HiveUtil hive;
  private Queue<Future<Void>> hiveUpdateFutures;
  private boolean hiveIntegration;

  /**
   * No-arg constructor. Used by Connect framework.
   */
  public S3SinkTask() {
    // no-arg constructor required by Connect framework.
    assignment = new HashSet<>();
    topicPartitionWriters = new HashMap<>();
    time = new SystemTime();
  }

  // visible for testing.
  S3SinkTask(S3SinkConnectorConfig connectorConfig, SinkTaskContext context, S3Storage storage,
             Partitioner<FieldSchema> partitioner, Format<S3SinkConnectorConfig, String> format,
             Time time) throws Exception {
    this.assignment = new HashSet<>();
    this.topicPartitionWriters = new HashMap<>();
    this.connectorConfig = connectorConfig;
    this.context = context;
    this.storage = storage;
    this.partitioner = partitioner;
    this.format = format;
    this.time = time;

    url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
    writerProvider = this.format.getRecordWriterProvider();
    schemaFileReader = this.format.getSchemaFileReader();

    open(context.assignment());
    log.info("Started S3 connector task with assigned partitions {}", assignment);
  }

  public void start(Map<String, String> props) {
    try {
      connectorConfig = new S3SinkConnectorConfig(props);
      url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);

      @SuppressWarnings("unchecked")
      Class<? extends S3Storage> storageClass =
          (Class<? extends S3Storage>)
              connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
      storage = StorageFactory.createStorage(
          storageClass,
          S3SinkConnectorConfig.class,
          connectorConfig,
          url
      );
      if (!storage.bucketExists()) {
        throw new DataException("No-existent S3 bucket: " + connectorConfig.getBucketName());
      }
      topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);

      executorService = Executors.newSingleThreadExecutor();
      setupHiveIntegration();
      checkScheduledRotationTimeZone();

      writerProvider = newFormat().getRecordWriterProvider();
      partitioner = newPartitioner(connectorConfig);
      delim = (String) connectorConfig.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);

      for (TopicPartition tp : assignment) {
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
                tp,
                storage,
                writerProvider,
                partitioner,
                connectorConfig,
                context,
                hiveMetaStore,
                hive,
                schemaFileReader,
                executorService,
                hiveUpdateFutures,
                time
        );
        topicPartitionWriters.put(tp, topicPartitionWriter);
      }

      open(context.assignment());
      if (hiveIntegration) {
        syncWithHive();
      }
      log.info("Started S3 connector task with assigned partitions: {}", assignment);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | InvocationTargetException | NoSuchMethodException e) {
      throw new ConnectException("Reflection exception: ", e);
    } catch (AmazonClientException e) {
      throw new ConnectException(e);
    }
  }

  /**
   * Check that timezone it setup correctly in case of scheduled rotation
   */
  protected void checkScheduledRotationTimeZone() {
    if (connectorConfig.getLong(StorageSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG) > 0) {
      String timeZoneString = connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG);
      if (StringUtils.isBlank(timeZoneString)) {
        throw new ConfigException(PartitionerConfig.TIMEZONE_CONFIG,
                timeZoneString, "Timezone cannot be empty when using scheduled file rotation."
        );
      }
      DateTimeZone.forID(timeZoneString);
    }
  }

  /**
   * Sets up Hive Integration
   * @return true if Hive integration is enabled
   */
  protected boolean setupHiveIntegration() {
    hiveIntegration = connectorConfig.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG);
    if (hiveIntegration) {
      StorageSchemaCompatibility compatibility = StorageSchemaCompatibility.getCompatibility(
              connectorConfig.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG)
      );
      if (compatibility == StorageSchemaCompatibility.NONE) {
        throw new ConfigException(
                "Hive Integration requires schema compatibility to be BACKWARD, FORWARD or FULL"
        );
      }
      hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);
      hiveMetaStore = new HiveMetaStore(connectorConfig);
      hive = format.getHiveFactory().createHiveUtil(connectorConfig, hiveMetaStore);
      hiveUpdateFutures = new LinkedList<>();
    }
    return hiveIntegration;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    // assignment should be empty, either because this is the initial call or because it follows
    // a call to "close".
    assignment.addAll(partitions);
    for (TopicPartition tp : assignment) {
      TopicPartitionWriter writer = new TopicPartitionWriter(
          tp,
          storage,
          writerProvider,
          partitioner,
          connectorConfig,
          context,
          time
      );
      topicPartitionWriters.put(tp, writer);
    }
  }

  @SuppressWarnings("unchecked")
  private Format<S3SinkConnectorConfig, String> newFormat()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
             InvocationTargetException, NoSuchMethodException {
    Class<Format<S3SinkConnectorConfig, String>> formatClass =
        (Class<Format<S3SinkConnectorConfig, String>>) connectorConfig.getClass(
            S3SinkConnectorConfig.FORMAT_CLASS_CONFIG
        );
    return formatClass.getConstructor(S3Storage.class).newInstance(storage);
  }

  private Partitioner<FieldSchema> newPartitioner(S3SinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    @SuppressWarnings("unchecked")
    Class<? extends Partitioner<FieldSchema>> partitionerClass =
        (Class<? extends Partitioner<FieldSchema>>)
            config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);

    Partitioner<FieldSchema> partitioner = partitionerClass.newInstance();

    Map<String, Object> plainValues = new HashMap<>(config.plainValues());
    Map<String, ?> originals = config.originals();
    for (String originalKey : originals.keySet()) {
      if (!plainValues.containsKey(originalKey)) {
        // pass any additional configs down to the partitioner so that custom partitioners can
        // have their own configs
        plainValues.put(originalKey, originals.get(originalKey));
      }
    }
    partitioner.configure(plainValues);

    return partitioner;
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionWriters.get(tp).buffer(record);
    }
    if (log.isDebugEnabled()) {
      log.debug("Read {} records from Kafka", records.size());
    }

    if (hiveIntegration) {
      Iterator<Future<Void>> iterator = hiveUpdateFutures.iterator();
      while (iterator.hasNext()) {
        try {
          Future<Void> future = iterator.next();
          if (future.isDone()) {
            future.get();
            iterator.remove();
          } else {
            break;
          }
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    for (TopicPartition tp : assignment) {
      topicPartitionWriters.get(tp).write();
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // No-op. The connector is managing the offsets.
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> offsets
  ) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    for (TopicPartition tp : assignment) {
      Long offset = topicPartitionWriters.get(tp).getOffsetToCommitAndReset();
      if (offset != null) {
        log.trace("Forwarding to framework request to commit offset: {} for {}", offset, tp);
        offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
      }
    }
    return offsetsToCommit;
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : assignment) {
      try {
        topicPartitionWriters.get(tp).close();
      } catch (ConnectException e) {
        log.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
      }
    }
    topicPartitionWriters.clear();
    assignment.clear();
  }

  @Override
  public void stop() {
    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  private void syncWithHive() throws ConnectException {
    Set<String> topics = new HashSet<>();
    for (TopicPartition tp : assignment) {
      topics.add(tp.topic());
    }

    for (String topic : topics) {
      String topicDir = Utils.join(Arrays.asList(url, topicsDir, topic), delim);
//        CommittedFileFilter filter = new TopicCommittedFileFilter(topic);
      String keyWithMaxOffset = FileUtils.keyWithMaxOffset(
              storage,
              topicDir
      );
      if (keyWithMaxOffset != null) {
//          final Path path = fileStatusWithMaxOffset.getPath();
        final Schema latestSchema;
        latestSchema = schemaFileReader.getSchema(
                connectorConfig,
                keyWithMaxOffset
        );
        hive.createTable(hiveDatabase, topic, latestSchema, partitioner);
        List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, topic, (short) -1);
//          FileStatus[] statuses = FileUtils.getDirectories(storage, new Path(topicDir));
        String[] keys = new String[] {}; // TODO: List S3 keys from bucket
        for (String location : keys) {
          if (!partitions.contains(location)) {
            String partitionValue = getPartitionValue(location);
            hiveMetaStore.addPartition(hiveDatabase, topic, partitionValue);
          }
        }
      }
    }
  }

  private String getPartitionValue(String path) {
    String[] parts = path.split("/");
    return Utils.join(Arrays.copyOfRange(parts, 3, parts.length), "/");
  }

  // Visible for testing
  TopicPartitionWriter getTopicPartitionWriter(TopicPartition tp) {
    return topicPartitionWriters.get(tp);
  }

  // Visible for testing
  Format<S3SinkConnectorConfig, String> getFormat() {
    return format;
  }
}
