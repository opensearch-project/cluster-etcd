/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A pull-based ingestion source held in a list, so that a test can be the stream.
 *
 * <p>Exists because a consumer-pointer reset cannot be observed any other way. Re-consuming a range rewrites the
 * same documents with the same content — every ingested message carries its own {@code _id}, which
 * {@code MessageProcessorRunnable} insists on — so a reset that worked and a reset that did nothing leave an
 * identical index, with an identical document count. The only way to see one is to **change the payload behind a
 * stable id and then rewind**, which is what {@link #rewrite} is for and what a real operator did to this
 * plugin's `ingestion-fs` source to confirm the feature by hand. Kafka would not allow it; a list does.
 *
 * <p>Offsets are list indices, 0-based, matching the file source — so offset 2 is the third message, which is the
 * off-by-one that reads as a reset landing in the wrong place when it landed exactly where it was sent.
 *
 * <p>Modelled on the server's own {@code FakeIngestionSource}, which is in OpenSearch's test sources and not
 * published, so it cannot be depended on from here.
 */
public class ListIngestionSource {

    /** The stream, mutable, shared with whatever consumers the engine creates. */
    private static final List<byte[]> MESSAGES = new CopyOnWriteArrayList<>();

    public static void reset() {
        MESSAGES.clear();
    }

    /** Appends a document to the stream, at the next offset. */
    public static void publish(String id, String title) {
        MESSAGES.add(document(id, title));
    }

    /**
     * Replaces the payload at an offset, keeping the {@code _id}.
     *
     * <p>The whole point of this class: after this, a copy that re-consumes that offset writes different content
     * under the same id, and a search can tell whether it did.
     */
    public static void rewrite(int offset, String id, String title) {
        MESSAGES.set(offset, document(id, title));
    }

    public static int size() {
        return MESSAGES.size();
    }

    private static byte[] document(String id, String title) {
        return ("{\"_id\":\"" + id + "\",\"_source\":{\"title\":\"" + title + "\"}}").getBytes(StandardCharsets.UTF_8);
    }

    /** The plugin an integration test installs to make {@code index.ingestion_source.type: list} resolvable. */
    public static class TestPlugin extends Plugin implements IngestionConsumerPlugin {

        @Override
        @SuppressWarnings("rawtypes")
        public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
            return Map.of("LIST", new ListConsumerFactory());
        }

        @Override
        public String getType() {
            return "LIST";
        }
    }

    /** One consumer per shard, all reading the one list. */
    public static class ListConsumerFactory implements IngestionConsumerFactory<ListConsumer, ListPointer> {

        @Override
        public ListConsumer createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata) {
            return new ListConsumer(shardId);
        }

        @Override
        public ListPointer parsePointerFromString(String pointer) {
            return new ListPointer(Long.parseLong(pointer));
        }
    }

    /** Reads from the shared list. Every shard sees every message, which is fine for a one-shard index. */
    public static class ListConsumer implements IngestionShardConsumer<ListPointer, ListMessage> {

        private final int shardId;
        private long lastFetchedOffset = -1;

        ListConsumer(int shardId) {
            this.shardId = shardId;
        }

        @Override
        public List<ReadResult<ListPointer, ListMessage>> readNext(
            ListPointer pointer,
            boolean includeStart,
            long maxMessages,
            int timeoutMillis
        ) {
            long from = includeStart ? pointer.offset : pointer.offset + 1;
            lastFetchedOffset = from - 1;

            List<ReadResult<ListPointer, ListMessage>> results = new ArrayList<>();
            for (long offset = from; offset < MESSAGES.size() && results.size() < maxMessages; offset++) {
                results.add(new ReadResult<>(new ListPointer(offset), new ListMessage(MESSAGES.get((int) offset))));
                lastFetchedOffset = offset;
            }
            return results;
        }

        @Override
        public List<ReadResult<ListPointer, ListMessage>> readNext(long maxMessages, int timeoutMillis) {
            return readNext(new ListPointer(lastFetchedOffset), false, maxMessages, timeoutMillis);
        }

        @Override
        public ListPointer earliestPointer() {
            return new ListPointer(0);
        }

        @Override
        public ListPointer latestPointer() {
            return new ListPointer(MESSAGES.size());
        }

        @Override
        public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
            throw new UnsupportedOperationException("a list has no timestamps; reset by offset");
        }

        @Override
        public IngestionShardPointer pointerFromOffset(String offset) {
            return new ListPointer(Long.parseLong(offset));
        }

        @Override
        public int getShardId() {
            return shardId;
        }

        @Override
        public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
            return 0;
        }

        @Override
        public void close() throws IOException {}
    }

    /** One message. The payload is read at consumption time, which is what makes {@link #rewrite} visible. */
    public static class ListMessage implements Message<byte[]> {

        private final byte[] payload;

        ListMessage(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public byte[] getPayload() {
            return payload;
        }

        @Override
        public Long getTimestamp() {
            return System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }

    /** An offset into the list, which is what {@code mode: offset} in an ingestion_reset resolves to. */
    public static class ListPointer implements IngestionShardPointer {

        private final long offset;

        public ListPointer(long offset) {
            this.offset = offset;
        }

        @Override
        public byte[] serialize() {
            return ByteBuffer.allocate(Long.BYTES).putLong(offset).array();
        }

        @Override
        public String asString() {
            return String.valueOf(offset);
        }

        @Override
        public String toString() {
            return asString();
        }

        @Override
        public Field asPointField(String fieldName) {
            return new LongPoint(fieldName, offset);
        }

        @Override
        public Query newRangeQueryGreaterThan(String fieldName) {
            return LongPoint.newRangeQuery(fieldName, offset, Long.MAX_VALUE);
        }

        @Override
        public int compareTo(IngestionShardPointer other) {
            return Long.compare(offset, ((ListPointer) other).offset);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ListPointer && ((ListPointer) other).offset == offset;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(offset);
        }
    }
}
