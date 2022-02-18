package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author lishanglin
 * date 2022/2/17
 */
@Service
public class DefaultRedisOpParserManager implements RedisOpParserManager {

    private Map<RedisOpType, RedisOpParser> parserMap = Maps.newConcurrentMap();

    private Logger logger = LoggerFactory.getLogger(DefaultRedisOpParserManager.class);

    @Override
    public synchronized void registerParser(RedisOpType opType, RedisOpParser parser) {
        if (!parserMap.containsKey(opType) || parserMap.get(opType).getOrder() > parser.getOrder()) {
            logger.info("[registerParser] {}", parser);
            parserMap.put(opType, parser);
        }
    }

    @Override
    public RedisOpParser findParser(RedisOpType opType) {
        return parserMap.get(opType);
    }

}
