package com.ctrip.xpipe.redis.proxy.compress;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultRunNotifier implements RunNotifier {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRunNotifier.class);

    private List<RunListener> listeners = Lists.newLinkedList();

    @Override
    public void addListener(RunListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(RunListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void fireCompressStarted() {
        new SafeNotifier() {
            @Override
            protected void notifyListener(RunListener each) throws Exception {
                each.testCompressStarted();
            }
        }.run();
    }

    @Override
    public void fireCompressFinished() {
        new SafeNotifier() {
            @Override
            protected void notifyListener(RunListener each) throws Exception {
                each.testCompressFinished();
            }
        }.run();
    }

    @Override
    public void fireDecompressStarted() {
        new SafeNotifier() {
            @Override
            protected void notifyListener(RunListener each) throws Exception {
                each.testDecompressStarted();
            }
        }.run();
    }

    @Override
    public void fireDecompressFinished() {
        new SafeNotifier() {
            @Override
            protected void notifyListener(RunListener each) throws Exception {
                each.testDecompressFinished();
            }
        }.run();
    }

    @Override
    public void registerResult(Result result) {
        new SafeNotifier() {
            @Override
            protected void notifyListener(RunListener each) throws Exception {
                each.statsToResult(result);
            }
        }.run();
    }

    private abstract class SafeNotifier {
        private final List<RunListener> currentListeners;

        SafeNotifier() {
            this(listeners);
        }

        SafeNotifier(List<RunListener> currentListeners) {
            this.currentListeners = currentListeners;
        }

        void run() {
            List<RunListener> safeListeners = Lists.newArrayList(currentListeners);
            for (RunListener listener : safeListeners) {
                try {
                    notifyListener(listener);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }

        protected abstract void notifyListener(RunListener each) throws Exception;
    }
}
