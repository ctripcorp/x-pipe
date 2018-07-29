package com.ctrip.xpipe.redis.proxy.echoserver;

import com.dianping.cat.Cat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Jul 09, 2018
 */
public class EchoClientStarter {

    private static final Logger logger = LoggerFactory.getLogger(EchoClientStarter.class);

    public static final String KEY_REMOTE_HOST = "echo.client.remote.host";

    public static final String KEY_REMOTE_PORT = "echo.client.remote.port";

    public static final String KEY_PROTOCOL = "echo.client.proxy.protocol";

    public static final String KEY_SPEED_FORCE = "echo.client.speed";

    public static final int MEGA_BYTE = 1024 * 1024;

    private static final ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws InterruptedException {
        String protocol = System.getProperty(KEY_PROTOCOL, "");
        String host = System.getProperty(KEY_REMOTE_HOST, "127.0.0.1");
        int port = Integer.parseInt(System.getProperty(KEY_REMOTE_PORT, "8009"));
        int speed = Integer.parseInt(System.getProperty(KEY_SPEED_FORCE, "" + MEGA_BYTE*10));
        doConnect(host, port, protocol, speed);
    }

    private static void doConnect(String host, int port, String protocol, int speed) {
        AdvancedEchoClient client = new AdvancedEchoClient(host, port, protocol, speed);

        Cat.logEvent("Start", System.nanoTime()+"");
        try {
            client.startServer().channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("[doConnect]", e);
            Cat.logError("ReConnected", e);
        } finally {
            scheduled.schedule(()->doConnect(host, port, protocol, speed), 1, TimeUnit.MINUTES);
        }

    }
}
