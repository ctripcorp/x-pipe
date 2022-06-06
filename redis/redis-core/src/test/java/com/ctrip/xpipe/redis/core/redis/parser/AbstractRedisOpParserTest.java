package com.ctrip.xpipe.redis.core.redis.parser;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.*;
import org.junit.Before;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/22
 */
public class AbstractRedisOpParserTest extends AbstractRedisTest {

    protected RedisOpParserManager redisOpParserManager;

    protected RedisOpParser parser;

    @Before
    public void setupAbstractRedisOpParserTest() {
        redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        parser = new GeneralRedisOpParser(redisOpParserManager);
        new RedisOpMsetParser(redisOpParserManager);
        new RedisOpDelParser(redisOpParserManager);
    }

    protected byte[][] strList2bytesArray(List<String> strList) {
        byte[][] byteArr = new byte[strList.size()][];
        for (int i = 0; i < strList.size(); i++) {
            byteArr[i] = strList.get(i).getBytes();
        }

        return byteArr;
    }

}
