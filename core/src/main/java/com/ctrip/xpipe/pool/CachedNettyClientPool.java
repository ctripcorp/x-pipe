package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * Sep 11, 2018
 */
public class CachedNettyClientPool implements SimpleObjectPool<NettyClient> {

    private SimpleObjectPool<NettyClient> objectPool;

    private AtomicReference<NettyClient> objRef = new AtomicReference<NettyClient>();

    private Semaphore semaphore;

    private int permits;

    public CachedNettyClientPool(SimpleObjectPool<NettyClient> objectPool) {
        this(objectPool, 1);
    }

    public CachedNettyClientPool(SimpleObjectPool<NettyClient> objectPool, int permits) {
        this.permits = permits;
        this.objectPool = objectPool;
        semaphore = new Semaphore(permits);
    }

    @Override
    public NettyClient borrowObject() throws BorrowObjectException {
        if(semaphore.tryAcquire()){
            return doBorrowObject();
        }else{
            return objectPool.borrowObject();
        }
    }

    @Override
    public void returnObject(NettyClient client) throws ReturnObjectException {
        if(objRef.get().equals(client)) {
            semaphore.release();
        } else {
            objectPool.returnObject(client);
        }
    }

    private NettyClient doBorrowObject () throws BorrowObjectException {
        if(objRef.get() == null) {
            synchronized (this) {
                if(objRef.get() == null) {
                    NettyClient client = objectPool.borrowObject();
                    client.channel().closeFuture().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            objRef.compareAndSet(client, null);
                            objectPool.returnObject(client);
                        }
                    });
                    objRef.set(client);
                    return client;
                }
            }
        }
        return objRef.get();
    }

    @Override
    public void clear() {
        semaphore = new Semaphore(permits);
    }

    @Override
    public String desc() {
        return objRef.get().toString();
    }
}
