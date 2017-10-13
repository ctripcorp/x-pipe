package com.ctrip.xpipe.redis.keeper.simple.load;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.keeper.simple.AbstractLoadRedis;
import redis.clients.jedis.Jedis;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         May 23, 2016
 */
public class SimpleSendMessage extends AbstractLoadRedis {

    private final int messageSize = 1 << 10;

    private int expire = 0;

    private String message;

    public SimpleSendMessage(InetSocketAddress master) {
        super(master);
        message = createMessage(messageSize);
    }

    private String createMessage(int messageSize) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageSize; i++) {
            sb.append('c');
        }
        return sb.toString();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        int concurrent = 10;
        CountDownLatch latch = new CountDownLatch(concurrent);

        for (int i = 0; i < concurrent; i++) {

            executors.execute(new AbstractExceptionLogTask() {
                @Override
                public void doRun() throws InterruptedException {
                    try {
                        try (Jedis jedis = new Jedis(master.getHostString(), master.getPort())) {
                            while (true) {
                                long index = increase();
                                if (index < 0) {
                                    logger.info("[doStart][index < 0, break]{}", index);
                                    break;
                                }
                                long keyIndex = getKeyIndex(index);
                                if (expire > 0) {
                                    jedis.setex(String.valueOf(keyIndex), expire, message);
                                } else {
                                    jedis.set(String.valueOf(keyIndex), message);
                                }

                                if(sleepMilli > 0){
                                    TimeUnit.MILLISECONDS.sleep(sleepMilli);
                                }
                            }
                        }
                        logger.info("[doStart][finish]");
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {

        SimpleSendMessage simpleSendMessage = new SimpleSendMessage(new InetSocketAddress("localhost", 6379));
        simpleSendMessage.total = 1 << 23;
        simpleSendMessage.sleepMilli = 10;
        simpleSendMessage.expire = 0;
        simpleSendMessage.setMaxKeyIndex(1 << 30);

        simpleSendMessage.initialize();
        simpleSendMessage.start();
    }

}
