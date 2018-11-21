package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.proxy.session.SessionEventHandler;
import com.ctrip.xpipe.utils.DateTimeUtils;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public interface SessionStats extends Startable, Stoppable, SessionEventHandler {

    void increaseInputBytes(long bytes);

    void increaseOutputBytes(long bytes);

    long lastUpdateTime();

    long getInputBytes();

    long getOutputBytes();

    long getInputInstantaneousBPS();

    long getOutputInstantaneousBPS();

    List<AutoReadEvent> getAutoReadEvents();

    class AutoReadEvent {
        private long startTime;
        private long endTime;

        public long getStartTime() {
            return startTime;
        }

        public AutoReadEvent setStartTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public long getEndTime() {
            return endTime;
        }

        public AutoReadEvent setEndTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        @Override
        public String toString() {
            return "AutoReadEvent{" +
                    "startTime=" + DateTimeUtils.timeAsString(startTime) +
                    ", endTime=" + DateTimeUtils.timeAsString(endTime) +
                    '}';
        }
    }
}
