package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.container.resp.UnclosedHandlesResp;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleCloser;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleTestSupport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * keeper-tfs-m6 P8 (spec §8): {@code GET /keepers/unclosed-handles} returns every pending handle as
 * plain values.
 */
public class KeeperContainerControllerUnclosedHandlesTest {

    private static final long WINDOW_MILLIS = 300_000L;

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private ContainerResourceManager containerResourceManager;
    private KeeperContainerController controller;

    @Before
    public void setUp() {
        containerResourceManager = mock(ContainerResourceManager.class);
        controller = new KeeperContainerController();
        ReflectionTestUtils.setField(controller, "containerResourceManager", containerResourceManager);
    }

    @Test
    public void testReturnsAllHandlesAsPlainValues() throws Exception {
        DeferredHandleCloser closer =
                DeferredHandleTestSupport.idleCloser(mock(AsyncFileSystem.class), clock::get, WINDOW_MILLIS);
        long writeDeferredAt = clock.get();
        DeferredHandleTestSupport.deferFile(closer, "/data/repl-1/meta.v2.json", true, ReplId.from(1L),
                "close meta", new IOException("Input/output error"));
        clock.addAndGet(10);
        DeferredHandleTestSupport.deferSegment(closer, "/data/repl-2", "cmd_", false, null, "close reader", null);
        when(containerResourceManager.getDeferredHandleCloser()).thenReturn(closer);

        UnclosedHandlesResp resp = controller.unclosedHandles();
        assertEquals(2, resp.getCount());
        assertEquals(1, resp.getWriteCount());
        assertEquals(2, resp.getHandles().size());

        UnclosedHandlesResp.Item write = resp.getHandles().get(0);
        assertEquals(ReplId.from(1L).toString(), write.getReplId());
        assertEquals("NUL rendered as |", "f|/data/repl-1/meta.v2.json", write.getFileKey());
        assertTrue(write.isWrite());
        assertEquals("close meta", write.getOperation());
        assertEquals("FLUSH", write.getMode());
        assertEquals(writeDeferredAt, write.getFirstFailureTime());
        assertEquals(writeDeferredAt + WINDOW_MILLIS, write.getFlushDeadline());
        assertEquals(writeDeferredAt + 1000L, write.getNextRetryTime());
        assertEquals(0, write.getAttempts());
        assertTrue(write.getAge() >= 0);
        assertEquals(IOException.class.getName(), write.getLastErrorType());
        assertEquals("Input/output error", write.getLastErrorMessage());

        UnclosedHandlesResp.Item read = resp.getHandles().get(1);
        assertEquals("unknown", read.getReplId());
        assertEquals("s|/data/repl-2|cmd_", read.getFileKey());
        assertFalse(read.isWrite());
        assertEquals("NO_FLUSH", read.getMode());
        assertEquals(0L, read.getFlushDeadline());
        assertNull(read.getLastErrorType());
        assertNull(read.getLastErrorMessage());

        // Serialized form: exactly the documented fields, no FS / file object.
        JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(resp));
        assertEquals(Sets.newHashSet("count", "writeCount", "handles"), fieldNames(json));
        assertEquals(Sets.newHashSet("replId", "fileKey", "write", "operation", "mode", "firstFailureTime",
                "flushDeadline", "nextRetryTime", "attempts", "age", "lastErrorType", "lastErrorMessage"),
                fieldNames(json.get("handles").get(0)));
        for (JsonNode item : json.get("handles")) {
            item.fields().forEachRemaining(field -> assertTrue(field.getKey() + " must be a plain value",
                    field.getValue().isValueNode()));
        }
    }

    @Test
    public void testNoContainerFsReturnsEmpty() {
        when(containerResourceManager.getDeferredHandleCloser()).thenReturn(null);
        UnclosedHandlesResp resp = controller.unclosedHandles();
        assertEquals(0, resp.getCount());
        assertEquals(0, resp.getWriteCount());
        assertTrue(resp.getHandles().isEmpty());
    }

    private static Set<String> fieldNames(JsonNode node) {
        return Sets.newHashSet(node.fieldNames());
    }
}
