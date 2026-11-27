package com.ctrip.xpipe.redis.console.service.logicalbu;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.HttpStatusCodeException;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.function.Supplier;

/**
 * Every external call of keeper-ops goes through here, so a failure is classified exactly once
 * into (layer, code) at the place where we still know which dependency was being called.
 */
public final class LayeredCaller {

    private static final Logger logger = LoggerFactory.getLogger(LayeredCaller.class);

    public static final String DB_TARGET = "xpipe-db";

    private LayeredCaller() {
    }

    public static <T> T db(OpStage stage, String scope, Supplier<T> call) {
        try {
            return call.get();
        } catch (KeeperOpException e) {
            throw e;
        } catch (Throwable th) {
            OpFailure failure = classifyDb(stage, scope, th);
            logger.warn("[db]{}", failure, th);
            throw new KeeperOpException(failure, th);
        }
    }

    public static <T> T meta(OpStage stage, String dc, String scope, Supplier<T> call) {
        return metaAt(stage, "metaserver@" + dc, scope, call);
    }

    /** Same as {@link #meta} with an explicit target, e.g. the MetaServer address actually called. */
    public static <T> T metaAt(OpStage stage, String target, String scope, Supplier<T> call) {
        try {
            return call.get();
        } catch (KeeperOpException e) {
            throw e;
        } catch (Throwable th) {
            OpFailure failure = classifyMetaAt(stage, target, scope, th);
            logger.warn("[meta]{}", failure, th);
            throw new KeeperOpException(failure, th);
        }
    }

    public static void metaRun(OpStage stage, String dc, String scope, Runnable call) {
        meta(stage, dc, scope, () -> {
            call.run();
            return null;
        });
    }

    /**
     * Console DAL wraps SQL errors as ServerException → DalException → SQLException → SocketException,
     * so the decision is made on the cause chain, not the top-level type.
     */
    public static OpFailure classifyDb(OpStage stage, String scope, Throwable th) {
        String detail = rootMessage(th);
        if (find(th, BadRequestException.class) != null) {
            return OpFailure.console(stage, OpErrorCode.BAD_REQUEST, scope, detail);
        }
        if (find(th, DataNotFoundException.class) != null) {
            return OpFailure.console(stage, OpErrorCode.DATA_NOT_FOUND, scope, detail);
        }
        if (find(th, SQLTimeoutException.class) != null || find(th, InterruptedIOException.class) != null) {
            return new OpFailure(OpLayer.CONSOLE_DB, stage, OpErrorCode.DB_TIMEOUT, DB_TARGET, scope, detail);
        }
        if (find(th, SQLTransientConnectionException.class) != null
                || find(th, SQLNonTransientConnectionException.class) != null
                || find(th, SQLRecoverableException.class) != null
                || find(th, ConnectException.class) != null
                || find(th, UnknownHostException.class) != null
                || find(th, NoRouteToHostException.class) != null) {
            return new OpFailure(OpLayer.CONSOLE_DB, stage, OpErrorCode.DB_UNAVAILABLE, DB_TARGET, scope, detail);
        }
        if (find(th, SQLException.class) != null || hasClassNamed(th, "org.unidal.dal.jdbc.DalException")) {
            return new OpFailure(OpLayer.CONSOLE_DB, stage, OpErrorCode.DB_ERROR, DB_TARGET, scope, detail);
        }
        return OpFailure.console(stage, OpErrorCode.INTERNAL_ERROR, scope, detail);
    }

    public static OpFailure classifyMeta(OpStage stage, String dc, String scope, Throwable th) {
        return classifyMetaAt(stage, "metaserver@" + dc, scope, th);
    }

    public static OpFailure classifyMetaAt(OpStage stage, String target, String scope, Throwable th) {
        HttpStatusCodeException http = find(th, HttpStatusCodeException.class);
        if (http != null) {
            String body = http.getResponseBodyAsString();
            return new OpFailure(OpLayer.META_SERVER, stage, OpErrorCode.META_HTTP_ERROR, target, scope,
                    "HTTP " + http.getRawStatusCode() + (body == null || body.isEmpty() ? "" : ": " + body));
        }
        String detail = rootMessage(th);
        // connect timeout = could not reach MetaServer at all; checked before generic timeouts
        if (isConnectTimeout(th)) {
            return new OpFailure(OpLayer.META_SERVER, stage, OpErrorCode.META_UNREACHABLE, target, scope, detail);
        }
        // remaining InterruptedIOException: read timeout, MetaServer accepted the request but did not answer in time
        if (find(th, InterruptedIOException.class) != null) {
            return new OpFailure(OpLayer.META_SERVER, stage, OpErrorCode.META_TIMEOUT, target, scope, detail);
        }
        if (find(th, ConnectException.class) != null
                || find(th, UnknownHostException.class) != null
                || find(th, NoRouteToHostException.class) != null
                || hasClassNamed(th, "org.springframework.web.client.ResourceAccessException")) {
            return new OpFailure(OpLayer.META_SERVER, stage, OpErrorCode.META_UNREACHABLE, target, scope, detail);
        }
        return new OpFailure(OpLayer.META_SERVER, stage, OpErrorCode.META_ERROR, target, scope, detail);
    }

    /** Console → KeeperContainer HTTP. {@code target} is the keepercontainer address. */
    public static <T> T keeper(OpStage stage, String target, String scope, Supplier<T> call) {
        try {
            return call.get();
        } catch (KeeperOpException e) {
            throw e;
        } catch (Throwable th) {
            OpFailure failure = classifyKeeper(stage, target, scope, th);
            logger.warn("[keeper]{}", failure, th);
            throw new KeeperOpException(failure, th);
        }
    }

    public static OpFailure classifyKeeper(OpStage stage, String target, String scope, Throwable th) {
        HttpStatusCodeException http = find(th, HttpStatusCodeException.class);
        if (http != null) {
            String body = http.getResponseBodyAsString();
            return new OpFailure(OpLayer.KEEPER, stage, OpErrorCode.KEEPER_HTTP_ERROR, target, scope,
                    "HTTP " + http.getRawStatusCode() + (body == null || body.isEmpty() ? "" : ": " + body));
        }
        String detail = rootMessage(th);
        if (isConnectTimeout(th)) {
            return new OpFailure(OpLayer.KEEPER, stage, OpErrorCode.KEEPER_UNREACHABLE, target, scope, detail);
        }
        if (find(th, InterruptedIOException.class) != null) {
            return new OpFailure(OpLayer.KEEPER, stage, OpErrorCode.KEEPER_TIMEOUT, target, scope, detail);
        }
        if (find(th, ConnectException.class) != null
                || find(th, UnknownHostException.class) != null
                || find(th, NoRouteToHostException.class) != null
                || hasClassNamed(th, "org.springframework.web.client.ResourceAccessException")) {
            return new OpFailure(OpLayer.KEEPER, stage, OpErrorCode.KEEPER_UNREACHABLE, target, scope, detail);
        }
        return new OpFailure(OpLayer.KEEPER, stage, OpErrorCode.KEEPER_ERROR, target, scope, detail);
    }
    static <E extends Throwable> E find(Throwable th, Class<E> type) {
        Throwable cur = th;
        int depth = 0;
        while (cur != null && depth++ < 20) {
            if (type.isInstance(cur)) {
                return type.cast(cur);
            }
            if (cur.getCause() == cur) {
                break;
            }
            cur = cur.getCause();
        }
        return null;
    }

    private static boolean isConnectTimeout(Throwable th) {
        if (hasClassNamed(th, "org.apache.http.conn.ConnectTimeoutException")) {
            return true;
        }
        Throwable cur = th;
        int depth = 0;
        while (cur != null && depth++ < 20) {
            if (cur instanceof java.net.SocketTimeoutException && cur.getMessage() != null
                    && cur.getMessage().toLowerCase().contains("connect timed out")) {
                return true;
            }
            if (cur.getCause() == cur) {
                break;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private static boolean hasClassNamed(Throwable th, String className) {
        Throwable cur = th;
        int depth = 0;
        while (cur != null && depth++ < 20) {
            for (Class<?> c = cur.getClass(); c != null; c = c.getSuperclass()) {
                if (c.getName().equals(className)) {
                    return true;
                }
            }
            if (cur.getCause() == cur) {
                break;
            }
            cur = cur.getCause();
        }
        return false;
    }

    static String rootMessage(Throwable th) {
        Throwable root = th;
        int depth = 0;
        while (root.getCause() != null && root.getCause() != root && depth++ < 20) {
            root = root.getCause();
        }
        String msg = root.getMessage();
        return root.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }
}
