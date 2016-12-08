package com.ctrip.xpipe.redis.integratedtest.function;

import com.google.common.util.concurrent.SettableFuture;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class LatencyTest {
    private static final long DEFAULT_TOTAL = 1 << 20;
    private static final int DEFAULT_PARTITION = 5;

    private long total;
    private int totalPartition;
    private long countIndicator;

    private DecimalFormat decimalFormat;
    private ExecutorService executorService;
    private String defaultKeyPrefix;
    private AtomicBoolean writeStarted;
    private AtomicBoolean readStarted;

    public LatencyTest(long total, int totalPartition) {
        executorService = Executors.newCachedThreadPool();
        writeStarted = new AtomicBoolean();
        readStarted = new AtomicBoolean();
        decimalFormat = new DecimalFormat("#.##");
        defaultKeyPrefix = String.format("lt-%s", new SimpleDateFormat("MMddHHmmss").format(new Date()));
        this.total = total;
        this.totalPartition = totalPartition;
        this.countIndicator = total / 10;
    }

    private void startWrite(String keyPrefix) {
        if (!writeStarted.compareAndSet(false, true)) {
            System.out.println("Write already started!");
            return;
        }

        if (keyPrefix == null || keyPrefix.trim().equals("")) {
            keyPrefix = defaultKeyPrefix;
        }

        System.out.println(String.format("Start writing %d records with keyPrefix: %s", total, keyPrefix));
        final String finalKeyPrefix = keyPrefix;
        final long partitionSize = total / totalPartition;
        for (int i = 0; i < totalPartition; i++) {
            final int count = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Jedis masterJedis = createJedis("localhost", 6379);
                    if (count == totalPartition - 1) {
                        doWrite(masterJedis, finalKeyPrefix, count * partitionSize, total);
                    } else {
                        doWrite(masterJedis, finalKeyPrefix, count * partitionSize, (count + 1) * partitionSize);
                    }

                }
            });
        }
    }

    private void doWrite(Jedis masterJedis, String keyPrefix, long startInclusive, long stopExclusive) {
        System.out.println(String.format("Writing [%d, %d)", startInclusive, stopExclusive));
        long start = System.currentTimeMillis();
        for (long i = startInclusive; i < stopExclusive; i++) {
            masterJedis.publish(String.format("%s-%d", keyPrefix, i), String.valueOf(System.currentTimeMillis()));
            //uncomment following code to control write speed
            /*if (i != 0 && i % 2000 == 0) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                }
            }*/
        }
        long stop = System.currentTimeMillis();
        double duration = (double) (stop - start) / (double) 1000;
        double qps = (stopExclusive - startInclusive) / duration;
        System.out.println(String.format("Write completed, estimated qps: %s", decimalFormat.format(qps)));
    }

    private void startRead(String keyPrefix) {
        if (!readStarted.compareAndSet(false, true)) {
            System.out.println("Read already started!");
            return;
        }

        if (keyPrefix == null || keyPrefix.trim().equals("")) {
            keyPrefix = defaultKeyPrefix;
        }

        System.out.println(String.format("Start reading %d records with keyPrefix: %s", total, keyPrefix));

        final String finalKeyPrefix = keyPrefix;
        final AtomicLong totalDelay = new AtomicLong();
        final AtomicLong counter = new AtomicLong();
        final SettableFuture<Boolean> readCompleted = SettableFuture.create();

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    readCompleted.get();
                    double avgDelay = (double) totalDelay.get() / (double) (total);
                    System.out.println(String.format("Read completed, average delay is: %s ms", decimalFormat.format
                            (avgDelay)));
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        });

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                final Jedis slaveJedis = createJedis("localhost", 6379);
                slaveJedis.psubscribe(new JedisPubSub() {
                    @Override
                    public void onPMessage(String pattern, String channel, String message) {
                        totalDelay.addAndGet(System.currentTimeMillis() - Long.valueOf(message));
                        long current = counter.incrementAndGet();
                        if (current != 0 && current % countIndicator == 0) {
                            System.out.println(String.format("%d records read", current));
                        }
                        if (current >= total) {
                            readCompleted.set(true);
                        }
                    }
                }, String.format("%s*", finalKeyPrefix));
            }
        });
    }

    private Jedis createJedis(String ip, int port) {
        Jedis jedis = new Jedis(ip, port);
        return jedis;
    }

    public static void main(String[] args) throws IOException {
        long total = DEFAULT_TOTAL;
        int totalPartition = DEFAULT_PARTITION;
        if (args.length == 2) {
            total = Long.parseLong(args[0]);
            totalPartition = Integer.parseInt(args[1]);
        }

        LatencyTest latencyTest = new LatencyTest(total, totalPartition);
        System.out.println(String.format(
                "X-pipe latency test. Total count: %d, partition: %d. Please input write or read to start.", total,
                totalPartition));
        while (true) {
            System.out.print("> ");
            String input = new BufferedReader(new InputStreamReader(System.in)).readLine();
            if (input == null || input.length() == 0) {
                continue;
            }
            input = input.trim();
            if (input.startsWith("write ")) {
                latencyTest.startWrite(input.substring(6));
                continue;
            }
            if (input.startsWith("write")) {
                latencyTest.startWrite(null);
                continue;
            }
            if (input.startsWith("read ")) {
                latencyTest.startRead(input.substring(5));
                continue;
            }
            if (input.startsWith("read")) {
                latencyTest.startRead(null);
                continue;
            }
            if (input.equalsIgnoreCase("quit")) {
                System.exit(0);
            }
        }
    }
}
