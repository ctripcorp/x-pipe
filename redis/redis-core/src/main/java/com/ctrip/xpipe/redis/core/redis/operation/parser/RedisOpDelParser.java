package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpDel;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/19
 */
@Component
public class RedisOpDelParser implements RedisOpParser {

    @Autowired
    public RedisOpDelParser(RedisOpParserManager redisOpParserManager) {
        redisOpParserManager.registerParser(RedisOpType.DEL, this);
    }

    @Override
    public RedisOp parse(List<String> args) {
        List<Pair<RedisKey, Void>> kvs = new ArrayList<>();

        for (int i = 1; i < args.size(); i++) {
            RedisKey key = new RedisKey(args.get(i));
            kvs.add(Pair.of(key, null));
        }

        return new RedisOpDel(args, kvs);
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
