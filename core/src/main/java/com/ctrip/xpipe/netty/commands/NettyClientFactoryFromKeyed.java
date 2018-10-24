package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;

/**
 * @author chen.zhu
 * <p>
 * Sep 13, 2018
 */
public class NettyClientFactoryFromKeyed implements PooledObjectFactory<NettyClient> {

    private Endpoint key;

    private NettyKeyedPoolClientFactory keyedPoolClientFactory;

    public NettyClientFactoryFromKeyed(Endpoint key, NettyKeyedPoolClientFactory keyedPoolClientFactory) {
        this.key = key;
        this.keyedPoolClientFactory = keyedPoolClientFactory;
    }

    @Override
    public PooledObject<NettyClient> makeObject() throws Exception {
        return keyedPoolClientFactory.makeObject(key);
    }

    @Override
    public void destroyObject(PooledObject<NettyClient> p) throws Exception {
        keyedPoolClientFactory.destroyObject(key, p);
    }

    @Override
    public boolean validateObject(PooledObject<NettyClient> p) {
        return keyedPoolClientFactory.validateObject(key, p);
    }

    @Override
    public void activateObject(PooledObject<NettyClient> p) throws Exception {
        keyedPoolClientFactory.activateObject(key, p);
    }

    @Override
    public void passivateObject(PooledObject<NettyClient> p) throws Exception {
        keyedPoolClientFactory.passivateObject(key, p);
    }

    @Override
    public String toString() {
        return key.toString();
    }
}
