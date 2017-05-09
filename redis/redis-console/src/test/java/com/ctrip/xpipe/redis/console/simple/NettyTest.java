package com.ctrip.xpipe.redis.console.simple;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         May 09, 2017
 */
public class NettyTest extends AbstractConsoleTest{

    @Test
    public void test(){

        List<ByteBuf> bufs = new LinkedList<>();

        int i = 0;
        while(true){

            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(1 << 20);
//            bufs.add(byteBuf);
            byteBuf.release();

            i++;
            System.out.println(i);
            sleep(1000);

        }


    }

}
