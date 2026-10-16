package com.ctrip.xpipe.redis.keeper.storage;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/** Captures the log events of one class's logger for assertions on content and stack traces. */
final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final AbstractAppender appender;
    final List<LogEvent> events = new CopyOnWriteArrayList<>();

    LogCapture(Class<?> loggerClass) {
        logger = (Logger) LogManager.getLogger(loggerClass);
        appender = new AbstractAppender("capture-" + loggerClass.getSimpleName(), null, null, true,
                Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        logger.addAppender(appender);
    }

    /** Events whose message contains {@code tag}, e.g. {@code [retry-fail]}. */
    List<LogEvent> withTag(String tag) {
        return events.stream().filter(e -> e.getMessage().getFormattedMessage().contains(tag))
                .collect(Collectors.toList());
    }

    static String message(LogEvent event) {
        return event.getMessage().getFormattedMessage();
    }

    static Level level(LogEvent event) {
        return event.getLevel();
    }

    @Override
    public void close() {
        logger.removeAppender(appender);
        appender.stop();
    }
}
