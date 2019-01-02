package com.ctrip.xpipe.redis.core.proxy.monitor;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

import java.util.Objects;

public class TunnelTrafficResult {

    private String tunnelId;

    private SessionTrafficResult frontend;

    private SessionTrafficResult backend;

    public TunnelTrafficResult(String tunnelId, SessionTrafficResult frontendResult,
                               SessionTrafficResult backendResult) {
        this.tunnelId = tunnelId;
        this.frontend = frontendResult;
        this.backend = backendResult;
    }

    public Object format() {
        Object[] frontendResult = frontend.toArray();
        Object[] backendResult = backend.toArray();
        Object[] result = new Object[3];
        result[0] = tunnelId;
        result[1] = frontendResult;
        result[2] = backendResult;
        return result;
    }

    public static TunnelTrafficResult parse(Object obj) {
        if(!obj.getClass().isArray()) {
            throw new XpipeRuntimeException("Illegal TunnelTrafficResult meta data, should be an array");
        }
        Object[] metaData = (Object[]) obj;
        if(!(metaData[0] instanceof String)) {
            throw new XpipeRuntimeException("Illegal TunnelTrafficResult meta data, first element should be string");
        }
        Object[] frontMeta = (Object[]) metaData[1];
        Object[] backMeta = (Object[]) metaData[2];
        return new TunnelTrafficResult((String)metaData[0],
                SessionTrafficResult.parseFromArray(frontMeta),
                SessionTrafficResult.parseFromArray(backMeta));
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public SessionTrafficResult getFrontend() {
        return frontend;
    }

    public SessionTrafficResult getBackend() {
        return backend;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunnelTrafficResult that = (TunnelTrafficResult) o;
        return Objects.equals(tunnelId, that.tunnelId) &&
                Objects.equals(frontend, that.frontend) &&
                Objects.equals(backend, that.backend);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tunnelId, frontend, backend);
    }
}
