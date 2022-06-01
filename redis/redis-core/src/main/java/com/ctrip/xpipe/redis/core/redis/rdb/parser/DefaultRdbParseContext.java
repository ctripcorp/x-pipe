package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/5/29
 */
public class DefaultRdbParseContext implements RdbParseContext {

    private EnumMap<RdbType, RdbParser> parsers = new EnumMap<>(RdbType.class);

    private Set<RdbParseListener> listeners = new HashSet<>();

    @Override
    public RdbParser getOrCreateParser(RdbType rdbType) {
        RdbParser parser = parsers.get(rdbType);
        if (null != parser) return parser;

        synchronized (this) {
            if (parsers.containsKey(rdbType)) return parsers.get(rdbType);
            RdbParser newParser = rdbType.makeParser(this);
            listeners.forEach(newParser::registerListener);
            parsers.put(rdbType, newParser);
            return parsers.get(rdbType);
        }
    }

    @Override
    public void registerListener(RdbParseListener listener) {
        if (listeners.contains(listener)) return;

        synchronized (this) {
            if (listeners.add(listener)) {
                parsers.values().forEach(parser -> parser.registerListener(listener));
            }
        }
    }

    @Override
    public void unregisterListener(RdbParseListener listener) {
        if (!listeners.contains(listener)) return;

        synchronized (this) {
            if (listeners.remove(listener)) {
                parsers.values().forEach(parser -> parser.unregisterListener(listener));
            }
        }
    }
}
