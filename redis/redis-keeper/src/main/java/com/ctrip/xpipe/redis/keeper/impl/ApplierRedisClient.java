package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @author lishanglin
 * date 2022/6/11
 */
public class ApplierRedisClient extends AbstractRedisClient<ApplierServer> implements RedisClient<ApplierServer> {

    private static final Logger logger = LoggerFactory.getLogger(ApplierRedisClient.class);

    public ApplierRedisClient(Channel channel, ApplierServer applierServer) {
        super(channel, applierServer);
    }

    @Override
    public RedisSlave becomeSlave() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RedisSlave becomeXSlave() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GapAllowRedisSlave becomeGapAllowRedisSlave() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientIpAddress(String host) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientIpAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String ip() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientEndpoint(Endpoint endpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Endpoint getClientEndpoint() {
        throw new UnsupportedOperationException();
    }

    // ---------------------------------------

    @Override
    public void setSlaveListeningPort(int port) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSlaveListeningPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void capa(CAPA capa) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean capaOf(CAPA capa) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<CAPA> getCapas() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isKeeper() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setKeeper() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void setIdc(String idc) {
    }

    @Override
    public String getIdc() {
        return null;
    }

    @Override
    public void setRegion(String region) {
    }

    @Override
    public String getRegion() {
        return null;
    }

    @Override
    public boolean isCrossRegion() {
        return false;
    }
}
