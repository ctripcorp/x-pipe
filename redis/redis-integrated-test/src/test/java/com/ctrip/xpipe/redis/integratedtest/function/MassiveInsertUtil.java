package com.ctrip.xpipe.redis.integratedtest.function;

import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class MassiveInsertUtil {
    private Jedis masterJedis;
    private String someKeyPrefix;

    public MassiveInsertUtil() {
        masterJedis = createJedis("localhost", 6379);
        someKeyPrefix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    }

    private Jedis createJedis(String ip, int port) {
        Jedis jedis = new Jedis(ip, port);
        return jedis;
    }

    public void insert(int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0 && i % 100 == 0) {
                System.out.println(String.format("%d keys inserted", i));
            }
            String key = String.format("%s-bulk-%d", someKeyPrefix, i);

            masterJedis.set(key, key);
        }
        System.out.println(String.format("Insert finished, totally %d keys inserted", count));
    }

    public static void main(String[] args) {
        MassiveInsertUtil insertUtil = new MassiveInsertUtil();
        insertUtil.insert(10000);
    }
}
