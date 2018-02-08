package io.confluent.connect.s3;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.util.AvroUtils;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileUtils {
    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    private static final int PATTERN_TOPIC_GROUP = 1;
    private static final int PATTERN_END_OFFSET_GROUP = 4;

    public static class KeySchemaHolder {
        private final S3Storage storage;
        private final String key;
        private final Pattern committedFilePattern;
        private Long offset;
        private Schema schema;

        KeySchemaHolder(S3Storage storage, String key) {
            this.storage = storage;
            this.key = key;
            S3SinkConnectorConfig conf = storage.conf();
            this.committedFilePattern = conf.getStorageCommonConfig().getCommittedFilePattern();
        }

        String getKey() {
            return key;
        }

        Long getOffset() {
            if (offset == null) {
                offset = extractOffset(committedFilePattern, key);
            }
            return offset;
        }

        Schema getSchema() throws IOException {
            if (schema == null) {
                schema = AvroUtils.getSchema(storage.open(key));
            }
            return schema;
        }
    }

    /**
     * Get all latest offsets for {@link StorageCommonConfig#TOPICS_DIR_CONFIG}
     * @param storage S3 Storage object
     * @return Mapping from (topic => {@link KeySchemaHolder}) where each value contains
     * the object for the max offset of that topic.
     */
    public static Map<String, KeySchemaHolder> getOffsetsForTopicsDir(S3Storage storage) {

        String pathPrefix = (String) storage.conf().get(StorageCommonConfig.TOPICS_DIR_CONFIG);
        if (pathPrefix == null) {
            pathPrefix = StorageCommonConfig.TOPICS_DIR_DEFAULT;
        }
        if (!storage.exists(pathPrefix)) {
            return null;
        }

        final Pattern committedFilePatern = storage.conf()
                .getStorageCommonConfig().getCommittedFilePattern();

        long maxOffset = -1;
        Map<String, KeySchemaHolder> topicOffsets = new HashMap<>();

        for (ObjectListing result = storage.list(pathPrefix);
             result.isTruncated(); result = storage.nextList(result)) {
            String key, topic;
            Matcher m;
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                key = objectSummary.getKey();
                m = committedFilePatern.matcher(key);

                if (!m.find()) {
                    log.error(key + " does not match COMMITTED_FILENAME_PATTERN");
                    continue;
                }
                topic = m.group(PATTERN_TOPIC_GROUP);
                if (!topicOffsets.containsKey(topic)) {
                    maxOffset = -1;
                    topicOffsets.put(topic, null);
                }
                long offset = Long.parseLong(m.group(PATTERN_END_OFFSET_GROUP));
                if (offset > maxOffset) {
                    maxOffset = offset;
                    topicOffsets.put(topic, new KeySchemaHolder(storage, key));
                }
            }
            String nextMarker = result.getNextMarker();
            if (nextMarker != null) {
                log.trace("Next Listing Marker : {}", nextMarker);
            } else {
                log.trace("End of bucket listing: {}", pathPrefix);
            }
        }
        return topicOffsets;
    }

    public static String keyWithMaxOffset(
            S3Storage storage,
            String pathPrefix
    ) {
        if (!storage.exists(pathPrefix)) {
            return null;
        }

        final Pattern committedFilePatern = storage.conf()
                .getStorageCommonConfig().getCommittedFilePattern();

        long maxOffset = -1L;
        String keyWithMaxOffset = null;

        for (ObjectListing result = storage.list(pathPrefix);
             result.isTruncated(); result = storage.nextList(result)) {
            String key;
            Matcher m;
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                key = objectSummary.getKey();
                m = committedFilePatern.matcher(key);

                // S3 has no directories, all keys are a flat listing
                // from the path prefix
                log.trace("Checking path for max offset: {}", key);
                if (!m.find()) {
                    log.error(key + " does not match COMMITTED_FILENAME_PATTERN");
                    continue;
                }
                long offset = Long.parseLong(m.group(PATTERN_END_OFFSET_GROUP));
                if (offset > maxOffset) {
                    maxOffset = offset;
                    keyWithMaxOffset = key;
                }
            }

            String nextMarker = result.getNextMarker();
            if (nextMarker != null) {
                log.trace("Next Listing Marker : {}", nextMarker);
            } else {
                log.trace("End of bucket listing: {}", pathPrefix);
            }
        }
        return keyWithMaxOffset;
    }

    public static long extractOffset(Pattern committedFilePattern, String filename) {
        Matcher m = committedFilePattern.matcher(filename);
        // NB: if statement has side effect of enabling group() call
        if (!m.find()) {
            throw new IllegalArgumentException(filename + " does not match COMMITTED_FILENAME_PATTERN");
        }
        return Long.parseLong(m.group(PATTERN_END_OFFSET_GROUP));
    }

}
