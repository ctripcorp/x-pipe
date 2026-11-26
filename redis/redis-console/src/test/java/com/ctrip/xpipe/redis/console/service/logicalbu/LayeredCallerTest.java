package com.ctrip.xpipe.redis.console.service.logicalbu;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.unidal.dal.jdbc.DalException;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;

public class LayeredCallerTest {

    @Test
    public void testDbTimeoutThroughDalWrapping() {
        Throwable th = new ServerException("Load data failed.", new DalException("dal", new SQLTimeoutException("timeout")));
        assertDb(th, OpLayer.CONSOLE_DB, OpErrorCode.DB_TIMEOUT);
    }

    @Test
    public void testDbSocketReadTimeout() {
        Throwable th = new ServerException("x", new SQLException("link", new SocketTimeoutException("Read timed out")));
        assertDb(th, OpLayer.CONSOLE_DB, OpErrorCode.DB_TIMEOUT);
    }

    @Test
    public void testDbUnavailable() {
        assertDb(new ServerException("x", new SQLNonTransientConnectionException("Communications link failure")),
                OpLayer.CONSOLE_DB, OpErrorCode.DB_UNAVAILABLE);
        assertDb(new ServerException("x", new SQLException("x", new ConnectException("refused"))),
                OpLayer.CONSOLE_DB, OpErrorCode.DB_UNAVAILABLE);
    }

    @Test
    public void testDbOtherSqlError() {
        assertDb(new ServerException("x", new SQLException("syntax")), OpLayer.CONSOLE_DB, OpErrorCode.DB_ERROR);
    }

    @Test
    public void testNonDbErrorIsConsole() {
        assertDb(new IllegalStateException("bug"), OpLayer.CONSOLE, OpErrorCode.INTERNAL_ERROR);
    }

    @Test
    public void testMetaTimeout() {
        assertMeta(new ResourceAccessException("I/O", new SocketTimeoutException("Read timed out")), OpErrorCode.META_TIMEOUT);
    }

    @Test
    public void testMetaUnreachable() {
        assertMeta(new ResourceAccessException("I/O", new ConnectException("Connection refused")), OpErrorCode.META_UNREACHABLE);
    }

    @Test
    public void testMetaConnectTimeoutIsUnreachable() {
        assertMeta(new ResourceAccessException("I/O", new SocketTimeoutException("Connect timed out")), OpErrorCode.META_UNREACHABLE);
    }

    @Test
    public void testMetaHttpError() {
        OpFailure f = LayeredCaller.classifyMeta(OpStage.NOTIFY_META, "SHAJQ", "SHAJQ/c1",
                new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR));
        Assert.assertEquals(OpErrorCode.META_HTTP_ERROR, f.getCode());
        Assert.assertTrue(f.getDetail(), f.getDetail().startsWith("HTTP 500"));
        Assert.assertEquals("metaserver@SHAJQ", f.getTarget());
    }

    @Test
    public void testWrapperRethrowsClassifiedException() {
        try {
            LayeredCaller.db(OpStage.LOAD, null, () -> {
                throw new ServerException("x", new SQLTimeoutException("t"));
            });
            Assert.fail();
        } catch (KeeperOpException e) {
            Assert.assertEquals(OpErrorCode.DB_TIMEOUT, e.getFailure().getCode());
            Assert.assertEquals(OpStage.LOAD, e.getFailure().getStage());
        }
    }

    private void assertDb(Throwable th, OpLayer layer, OpErrorCode code) {
        OpFailure f = LayeredCaller.classifyDb(OpStage.LOAD, null, th);
        Assert.assertEquals(f.toString(), layer, f.getLayer());
        Assert.assertEquals(f.toString(), code, f.getCode());
    }

    private void assertMeta(Throwable th, OpErrorCode code) {
        OpFailure f = LayeredCaller.classifyMeta(OpStage.NOTIFY_META, "SHAJQ", "SHAJQ/c1/s1", th);
        Assert.assertEquals(f.toString(), OpLayer.META_SERVER, f.getLayer());
        Assert.assertEquals(f.toString(), code, f.getCode());
    }
}
