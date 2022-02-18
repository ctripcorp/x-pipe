package com.ctrip.xpipe.redis.core.redis.operation;

import org.springframework.core.Ordered;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public interface RedisOpParser extends Ordered {

    RedisOp parse(List<String> args);

}
