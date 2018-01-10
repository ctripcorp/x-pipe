package com.ctrip.xpipe.redis.integratedtest.simple;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.dianping.cat.Cat;
import org.junit.Test;
import org.unidal.helper.Files;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2016
 */
public class SimpleTest extends AbstractSimpleTest {

    @Test
    public void testCat() {

        logger.info("begin:{}");
        Cat.newTransaction("type", "name");
        logger.info("end:{}");
    }

    @Test
    public void test() throws IOException {


        String file = "//Users/mengwenchao/tmp/tmpdir/file1";
        ReplicationStoreMeta meta = new ReplicationStoreMeta();

        long begin = System.currentTimeMillis();

//        String json = JSON.toJSONString(meta);
        long endJson = System.currentTimeMillis();
        logger.info("[begin]");
        Files.IO.INSTANCE.writeTo(new File(file), JSON.toJSONString(meta));
        logger.info("[end]");

        long end = System.currentTimeMillis();
        logger.info("cost json:{}, costSave:{} ", endJson - begin, end - endJson);
        logger.info("total:{}", end - begin);


        System.out.println("nihaoma");
    }

    @Test
    public void testAlloc() throws InterruptedException {

        while (true) {

            TimeUnit.MILLISECONDS.sleep(1);
            @SuppressWarnings("unused")
            byte[] data = new byte[1 << 10];
        }

    }


}
