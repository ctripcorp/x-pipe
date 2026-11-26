package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.keeper.container.resp.TailCacheFileMemoryListResp;
import com.ctrip.xpipe.redis.keeper.container.resp.TailCacheMemoryResp;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileMemoryStats;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystem;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Read-only TailCache diagnostics. Never creates the container FS.
 */
@RestController
@RequestMapping("/tfs")
public class TfsController extends AbstractController {

    static final String SORT_BY_UNDURABLE = "undurable";
    static final String SORT_BY_CACHE_SIZE = "cacheSize";
    static final String DEFAULT_FILE_LIMIT = "100";

    private static final Comparator<TailCacheFileMemoryStats> BY_PATH =
            Comparator.comparing(TailCacheFileMemoryStats::getPath);
    private static final Comparator<TailCacheFileMemoryStats> BY_UNDURABLE =
            Comparator.comparingLong(TailCacheFileMemoryStats::getUnDurableBytes).reversed().thenComparing(BY_PATH);
    private static final Comparator<TailCacheFileMemoryStats> BY_CACHE_SIZE =
            Comparator.comparingLong(TailCacheFileMemoryStats::getCacheSizeBytes).reversed().thenComparing(BY_PATH);

    @Autowired
    private ContainerResourceManager containerResourceManager;

    @GetMapping("/tail-cache/memory")
    public TailCacheMemoryResp tailCacheMemory() {
        logger.info("[tailCacheMemory]");
        Optional<TailCacheFileSystem> fs = containerResourceManager.getExistingTailCacheFileSystem();
        if (!fs.isPresent()) {
            return TailCacheMemoryResp.notInitialized(System.currentTimeMillis());
        }
        try {
            return TailCacheMemoryResp.from(fs.get().getMemoryStats());
        } catch (RuntimeException e) {
            logger.error("[tailCacheMemory] snapshot failed", e);
            throw e;
        }
    }

    @GetMapping("/tail-cache/files")
    public ResponseEntity<Object> tailCacheFiles(@RequestParam(defaultValue = SORT_BY_UNDURABLE) String sortBy,
                                                 @RequestParam(defaultValue = DEFAULT_FILE_LIMIT) int limit) {
        logger.info("[tailCacheFiles] sortBy={}, limit={}", sortBy, limit);
        Comparator<TailCacheFileMemoryStats> order = comparatorOf(sortBy);
        if (order == null) {
            return badRequest("sortBy must be " + SORT_BY_UNDURABLE + " or " + SORT_BY_CACHE_SIZE + ": " + sortBy);
        }
        if (limit <= 0) {
            return badRequest("limit must be positive: " + limit);
        }
        Optional<TailCacheFileSystem> fs = containerResourceManager.getExistingTailCacheFileSystem();
        long now = System.currentTimeMillis();
        if (!fs.isPresent()) {
            return ResponseEntity.ok(TailCacheFileMemoryListResp.notInitialized(now));
        }
        try {
            List<TailCacheFileMemoryStats> rows = new ArrayList<>(fs.get().listFileMemoryStats());
            rows.sort(order);
            int total = rows.size();
            List<TailCacheFileMemoryStats> limited = total > limit ? rows.subList(0, limit) : rows;
            return ResponseEntity.ok(TailCacheFileMemoryListResp.from(limited, total, now));
        } catch (RuntimeException e) {
            logger.error("[tailCacheFiles] snapshot failed, sortBy={}, limit={}", sortBy, limit, e);
            throw e;
        }
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Object> argumentTypeMismatch(MethodArgumentTypeMismatchException e) {
        return badRequest("invalid " + e.getName() + ": " + e.getValue());
    }

    private static Comparator<TailCacheFileMemoryStats> comparatorOf(String sortBy) {
        if (SORT_BY_UNDURABLE.equalsIgnoreCase(sortBy)) {
            return BY_UNDURABLE;
        }
        if (SORT_BY_CACHE_SIZE.equalsIgnoreCase(sortBy)) {
            return BY_CACHE_SIZE;
        }
        return null;
    }

    private ResponseEntity<Object> badRequest(String message) {
        logger.warn("[tfs] bad request: {}", message);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(message);
    }
}
