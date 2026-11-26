package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.metaserver.DcMetaReloadResult;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaReloader;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestOperations;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Make every MetaServer of this dc apply fresh DcMeta now, with ONE console getDcMeta per dc:
 *
 * <ol>
 *     <li>{@code POST /api/dcmeta/reload} (called by console): this instance pulls DcMeta from console once
 *     (one retry), encodes it, then applies it locally and POSTs the same JSON to every peer in parallel.</li>
 *     <li>{@code POST /api/dcmeta/apply?loadTime=} (peer to peer): apply the given DcMeta, no console call.</li>
 * </ol>
 * Both apply through {@link DcMetaReloader#apply}, the same compare/apply path as the periodic refresh, so
 * META_MODIFY_PROTECT_COUNT, the meta.sync.delay window and the rate-limited priority re-election all behave the same.
 * The response has one {@link DcMetaReloadResult} per instance; it returns once meta is applied, not once keepers
 * switched.
 */
@RestController
public class DcMetaReloadController {

    private static final Logger logger = LoggerFactory.getLogger(DcMetaReloadController.class);

    public static final String PATH_APPLY = "/api/dcmeta/apply";

    static final int PULL_RETRY = 1;
    static final long PULL_RETRY_INTERVAL_MILLI = 1000;
    static final int PEER_SO_TIMEOUT_MILLI = 30_000;

    @Autowired
    private DcMetaReloader dcMetaReloader;

    @Autowired
    private ClusterServers<?> clusterServers;

    @Autowired
    private CurrentClusterServer currentClusterServer;

    private final ExecutorService peerExecutor = Executors.newFixedThreadPool(8, XpipeThreadFactory.create("DcMetaReloadPeer"));

    private volatile RestOperations peerRestTemplate;

    @PreDestroy
    public void destroy() {
        peerExecutor.shutdownNow();
    }

    @RequestMapping(path = DcMetaReloadResult.PATH_RELOAD, method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public List<DcMetaReloadResult> reload() {
        int currentId = currentClusterServer.getServerId();
        String currentAddress = address(currentClusterServer.getClusterInfo());
        Map<Integer, ClusterServerInfo> peers = new LinkedHashMap<>(clusterServers.allClusterServerInfos());
        peers.remove(currentId);

        DcMetaReloader.Pulled pulled;
        String json;
        try {
            pulled = pullWithRetry();
            // encode before applying: once applied the DcMeta is live and may be mutated by clusterModified
            json = JsonCodec.INSTANCE.encode(pulled.getDcMeta());
        } catch (Exception e) {
            // without DcMeta nobody can apply; report every instance so console sees the whole dc failed
            String message = "pull from console failed: " + rootMessage(e);
            List<DcMetaReloadResult> results = new ArrayList<>();
            results.add(failed(currentId, currentAddress, message));
            for (Map.Entry<Integer, ClusterServerInfo> peer : peers.entrySet()) {
                results.add(failed(peer.getKey(), address(peer.getValue()), message));
            }
            logger.warn("[reload]{}", results);
            return results;
        }

        Map<Integer, Future<DcMetaReloadResult>> futures = new LinkedHashMap<>();
        for (Map.Entry<Integer, ClusterServerInfo> peer : peers.entrySet()) {
            int serverId = peer.getKey();
            String address = address(peer.getValue());
            futures.put(serverId, peerExecutor.submit(() -> applyOnPeer(serverId, address, json, pulled.getLoadTime())));
        }

        List<DcMetaReloadResult> results = new ArrayList<>();
        results.add(applyLocal(currentId, currentAddress, pulled.getDcMeta(), pulled.getLoadTime()));
        for (Map.Entry<Integer, Future<DcMetaReloadResult>> entry : futures.entrySet()) {
            String address = address(peers.get(entry.getKey()));
            try {
                results.add(entry.getValue().get(PEER_SO_TIMEOUT_MILLI + 5000L, TimeUnit.MILLISECONDS));
            } catch (TimeoutException e) {
                entry.getValue().cancel(true);
                results.add(failed(entry.getKey(), address, "timeout"));
            } catch (ExecutionException e) {
                results.add(failed(entry.getKey(), address, rootMessage(e.getCause())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                results.add(failed(entry.getKey(), address, "interrupted"));
            }
        }
        logger.info("[reload]{}", results);
        return results;
    }

    /** Peer side: apply the DcMeta pulled by the instance console called. */
    @RequestMapping(path = PATH_APPLY, method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public DcMetaReloadResult apply(@RequestParam("loadTime") long loadTime, @RequestBody String dcMetaJson) {
        int serverId = currentClusterServer.getServerId();
        String address = address(currentClusterServer.getClusterInfo());
        DcMeta dcMeta;
        try {
            dcMeta = JsonCodec.INSTANCE.decode(dcMetaJson, DcMeta.class);
        } catch (Exception e) {
            return failed(serverId, address, "decode DcMeta failed: " + rootMessage(e));
        }
        return applyLocal(serverId, address, dcMeta, loadTime);
    }

    private DcMetaReloader.Pulled pullWithRetry() throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt <= PULL_RETRY; attempt++) {
            try {
                return dcMetaReloader.pull();
            } catch (Exception e) {
                last = e;
                logger.warn("[pull][attempt {}] fail", attempt, e);
                if (attempt < PULL_RETRY) {
                    sleep(PULL_RETRY_INTERVAL_MILLI);
                }
            }
        }
        throw last;
    }

    @VisibleForTesting
    DcMetaReloadResult applyLocal(int serverId, String address, DcMeta dcMeta, long loadTime) {
        try {
            DcMetaReloader.ReloadStatus status = dcMetaReloader.apply(dcMeta, loadTime);
            return new DcMetaReloadResult(serverId, address, DcMetaReloadResult.Status.valueOf(status.name()), null);
        } catch (Exception e) {
            logger.warn("[applyLocal] fail", e);
            return failed(serverId, address, rootMessage(e));
        }
    }

    private DcMetaReloadResult applyOnPeer(int serverId, String address, String json, long loadTime) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        DcMetaReloadResult result = peerRestTemplate().postForObject(
                "http://" + address + PATH_APPLY + "?loadTime=" + loadTime,
                new HttpEntity<>(json, headers), DcMetaReloadResult.class);
        if (result == null) {
            return failed(serverId, address, "empty response");
        }
        return result.setServerId(serverId).setAddress(address);
    }

    private RestOperations peerRestTemplate() {
        if (peerRestTemplate == null) {
            synchronized (this) {
                if (peerRestTemplate == null) {
                    peerRestTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(0, 10, 1200, PEER_SO_TIMEOUT_MILLI);
                }
            }
        }
        return peerRestTemplate;
    }

    private static void sleep(long milli) {
        try {
            TimeUnit.MILLISECONDS.sleep(milli);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static DcMetaReloadResult failed(int serverId, String address, String message) {
        return new DcMetaReloadResult(serverId, address, DcMetaReloadResult.Status.FAILED, message);
    }

    private static String address(ClusterServerInfo info) {
        return info == null ? null : info.getIp() + ":" + info.getPort();
    }

    private static String rootMessage(Throwable th) {
        if (th == null) {
            return null;
        }
        Throwable root = th;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
    }
}
