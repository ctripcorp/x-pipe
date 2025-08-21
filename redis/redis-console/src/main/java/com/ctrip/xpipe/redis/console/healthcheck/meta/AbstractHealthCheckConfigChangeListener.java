package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.ConsoleConfigListener;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Sep 12, 2018
 */

public abstract class AbstractHealthCheckConfigChangeListener<T> implements ConsoleConfigListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConsoleConfig consoleConfig;

    @PostConstruct
    public void postConstruct() {
        consoleConfig.register(this.supportsKeys(),this);
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        T oldVal = convert(oldValue);
        T newVal = convert(newValue);
        logger.info("[onChange] key: {}, oldValue: {}, newValue: {}", key, oldVal, newVal);
        doOnChange(oldVal, newVal);
    }

    protected abstract T convert(String value);

    protected abstract void doOnChange(T oldValue, T newValue);

    // Pair<Deleted, Added>
    protected Pair<Set<String>, Set<String>> getDiff(String[] previous, String[] current) {
        Set<String> pre = Sets.newHashSet(previous);
        Set<String> deleted = Sets.newHashSet(previous);
        Set<String> added = Sets.newHashSet(current);
        deleted.removeAll(added);
        added.removeAll(pre);
        return new Pair<>(deleted, added);
    }

}
