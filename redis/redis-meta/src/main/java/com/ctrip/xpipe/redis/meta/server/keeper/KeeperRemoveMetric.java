package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Metrics for keepers / TFS stores removed by the metaserver: realtime release/remove and the periodic
 * orphan-store gc. Every point is one keeper or one shard with value 1, so a sum counts shards. Points
 * that can share a timestamp carry a distinguishing tag (address / replId): same tags + same timestamp
 * is a single point in the backend.
 */
public class KeeperRemoveMetric {

    private static final Logger logger = LoggerFactory.getLogger(KeeperRemoveMetric.class);

    public static final String METRIC_REMOVE_KEEPER = "meta.keeper.remove";
    public static final String METRIC_ORPHAN_GC = "meta.keeper.orphan.gc";

    public static final String KEEPER_TYPE_BM = "BM";
    public static final String KEEPER_TYPE_TFS = "TFS";
    public static final String OP_RELEASE = "release";
    public static final String OP_REMOVE = "remove";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAIL = "FAIL";
    public static final String STATUS_SKIP = "SKIP";
    /** meta's gc call to the keeper timed out; the keeper may still have deleted some of the chunk */
    public static final String STATUS_TIMEOUT = "TIMEOUT";
    public static final String NONE = "none";

    private final MetricProxy metricProxy;

    public KeeperRemoveMetric(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
    }

    /** One realtime release/remove of one keeper, on command completion. */
    public void trackRemoveKeeper(KeeperMeta keeperMeta, boolean bm, boolean release, String tfsId, boolean success) {
        try {
            MetricData metricData = new MetricData(METRIC_REMOVE_KEEPER);
            metricData.setTimestampMilli(System.currentTimeMillis());
            metricData.setValue(success ? 1 : 0);
            metricData.setHostPort(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
            metricData.addTag("keeperType", bm ? KEEPER_TYPE_BM : KEEPER_TYPE_TFS);
            metricData.addTag("op", release ? OP_RELEASE : OP_REMOVE);
            metricData.addTag("tfsId", StringUtil.isEmpty(tfsId) ? NONE : tfsId);
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[trackRemoveKeeper] fail", th);
        }
    }

    /**
     * One orphan gc chunk: a point per replId, never one per chunk. A replId missing from the response
     * counts as FAIL; a failed call goes through {@link #trackOrphanGcCallFail}.
     */
    public void trackOrphanGc(String fsId, List<Long> replIds, List<GcOrphanStoreResult> results) {
        try {
            Map<Long, GcOrphanStoreResult.Status> statusByReplId = new HashMap<>();
            if (results != null) {
                for (GcOrphanStoreResult result : results) {
                    if (result != null && result.replId != null) {
                        statusByReplId.put(result.replId, result.status);
                    }
                }
            }
            long now = System.currentTimeMillis();
            for (Long replId : replIds) {
                GcOrphanStoreResult.Status status = statusByReplId.get(replId);
                writeOrphanGc(fsId, replId, orphanGcStatus(status), status == GcOrphanStoreResult.Status.DELETED ? 1 : 0, now);
            }
        } catch (Throwable th) {
            logger.debug("[trackOrphanGc] fail", th);
        }
    }

    /** The gc call itself failed: every replId of the chunk is TIMEOUT if the call timed out, else FAIL. */
    public void trackOrphanGcCallFail(String fsId, List<Long> replIds, Throwable cause) {
        try {
            String status = isTimeout(cause) ? STATUS_TIMEOUT : STATUS_FAIL;
            long now = System.currentTimeMillis();
            for (Long replId : replIds) {
                writeOrphanGc(fsId, replId, status, 0, now);
            }
        } catch (Throwable th) {
            logger.debug("[trackOrphanGcCallFail] fail", th);
        }
    }

    private void writeOrphanGc(String fsId, Long replId, String status, double value, long timestampMilli)
            throws MetricProxyException {
        MetricData metricData = new MetricData(METRIC_ORPHAN_GC);
        metricData.setTimestampMilli(timestampMilli);
        metricData.setValue(value);
        metricData.addTag("tfsId", fsId);
        metricData.addTag("replId", String.valueOf(replId));
        metricData.addTag("status", status);
        metricProxy.writeBinMultiDataPoint(metricData);
    }

    /** RestTemplate read/connect timeout arrives as ResourceAccessException wrapping SocketTimeoutException. */
    static boolean isTimeout(Throwable th) {
        for (Throwable t = th; t != null; t = t.getCause()) {
            if (t instanceof SocketTimeoutException || t instanceof TimeoutException) {
                return true;
            }
        }
        return false;
    }

    /** DELETED is the only real delete; NOT_FOUND/SKIPPED left nothing to delete or deferred it. */
    static String orphanGcStatus(GcOrphanStoreResult.Status status) {
        if (status == null) {
            return STATUS_FAIL;
        }
        switch (status) {
            case DELETED:
                return STATUS_SUCCESS;
            case NOT_FOUND:
            case SKIPPED:
                return STATUS_SKIP;
            default:
                return STATUS_FAIL;
        }
    }
}
