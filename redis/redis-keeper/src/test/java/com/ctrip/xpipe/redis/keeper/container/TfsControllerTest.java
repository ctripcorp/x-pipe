package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.keeper.container.resp.TailCacheFileMemoryListResp;
import com.ctrip.xpipe.redis.keeper.container.resp.TailCacheMemoryResp;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileMemoryStats;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileMemoryStats.Kind;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheMemoryStats;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class TfsControllerTest {

    private ContainerResourceManager containerResourceManager;
    private TailCacheFileSystem fs;
    private TfsController controller;

    @Before
    public void setUp() {
        containerResourceManager = mock(ContainerResourceManager.class);
        fs = mock(TailCacheFileSystem.class);
        controller = new TfsController();
        ReflectionTestUtils.setField(controller, "containerResourceManager", containerResourceManager);
    }

    @Test
    public void testMemory() {
        withFs();
        when(fs.getMemoryStats()).thenReturn(new TailCacheMemoryStats(123L, 1024, 512, 100));
        TailCacheMemoryResp resp = controller.tailCacheMemory();
        assertTrue(resp.isInitialized());
        assertEquals(123L, resp.getSnapshotTimeMillis());
        assertEquals(1024, resp.getMaxCacheSizeBytes());
        assertEquals(512, resp.getCommittedBytes());
        assertEquals(100, resp.getUnDurableBytes());
    }

    @Test
    public void testSortByUnDurableThenPath() {
        withFs(row("/b", 10, 5), row("/a", 30, 5), row("/c", 20, 9));
        assertEquals(Arrays.asList("/c", "/a", "/b"), paths(files("undurable", 100)));
    }

    @Test
    public void testSortByCacheSizeIgnoresCase() {
        withFs(row("/b", 10, 5), row("/a", 30, 5), row("/c", 10, 9));
        assertEquals(Arrays.asList("/a", "/b", "/c"), paths(files("CACHESIZE", 100)));
    }

    @Test
    public void testLimitTruncates() {
        withFs(row("/a", 1, 3), row("/b", 1, 2), row("/c", 1, 1));
        TailCacheFileMemoryListResp truncated = files("undurable", 2);
        assertEquals(3, truncated.getTotal());
        assertTrue(truncated.isTruncated());
        assertEquals(Arrays.asList("/a", "/b"), paths(truncated));

        TailCacheFileMemoryListResp all = files("undurable", 10_000);
        assertEquals(3, all.getTotal());
        assertFalse(all.isTruncated());
        assertEquals(3, all.getFiles().size());
    }

    @Test
    public void testBadRequest() {
        withFs();
        assertEquals(HttpStatus.BAD_REQUEST, controller.tailCacheFiles("size", 10).getStatusCode());
        assertEquals(HttpStatus.BAD_REQUEST, controller.tailCacheFiles("undurable", 0).getStatusCode());
        verify(fs, never()).listFileMemoryStats();
    }

    @Test
    public void testFsNotCreated() {
        when(containerResourceManager.getExistingTailCacheFileSystem()).thenReturn(Optional.empty());
        TailCacheMemoryResp memory = controller.tailCacheMemory();
        assertFalse(memory.isInitialized());
        assertEquals(0, memory.getMaxCacheSizeBytes());
        assertEquals(0, memory.getCommittedBytes());
        assertEquals(0, memory.getUnDurableBytes());

        TailCacheFileMemoryListResp list = files("undurable", 100);
        assertFalse(list.isInitialized());
        assertEquals(0, list.getTotal());
        assertTrue(list.getFiles().isEmpty());
        verify(containerResourceManager, never()).getAsyncFileSystem();
    }

    @Test
    public void testHttpDefaultsAndTypeMismatch() throws Exception {
        withFs(row("/a", 7, 3));
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller).build();
        mvc.perform(get("/tfs/tail-cache/files"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.initialized").value(true))
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.truncated").value(false))
                .andExpect(jsonPath("$.files[0].kind").value("FILE"))
                .andExpect(jsonPath("$.files[0].path").value("/a"))
                .andExpect(jsonPath("$.files[0].cacheSizeBytes").value(7))
                .andExpect(jsonPath("$.files[0].unDurableBytes").value(3));
        mvc.perform(get("/tfs/tail-cache/files").param("limit", "abc"))
                .andExpect(status().isBadRequest());
    }

    private void withFs(TailCacheFileMemoryStats... rows) {
        when(containerResourceManager.getExistingTailCacheFileSystem()).thenReturn(Optional.of(fs));
        when(fs.listFileMemoryStats()).thenReturn(Arrays.asList(rows));
    }

    private TailCacheFileMemoryListResp files(String sortBy, int limit) {
        ResponseEntity<Object> resp = controller.tailCacheFiles(sortBy, limit);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        return (TailCacheFileMemoryListResp) resp.getBody();
    }

    private static TailCacheFileMemoryStats row(String path, long cacheSizeBytes, long unDurableBytes) {
        return new TailCacheFileMemoryStats(Kind.FILE, path, cacheSizeBytes, unDurableBytes);
    }

    private static List<String> paths(TailCacheFileMemoryListResp resp) {
        return resp.getFiles().stream().map(TailCacheFileMemoryListResp.Item::getPath).collect(Collectors.toList());
    }
}
