package com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration;

import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

public interface MigrationSystemAvailableChecker {

    MigrationSystemAvailability getResult();

    public static class MigrationSystemAvailability {

        private boolean avaiable;

        private boolean warning = false;

        private long timestamp = System.currentTimeMillis();

        private String message;

        private Map<String, CheckResult> checkResults;

        public MigrationSystemAvailability(boolean avaiable, String message) {
            this.avaiable = avaiable;
            this.message = message;
            this.checkResults = Maps.newConcurrentMap();
        }

        public static MigrationSystemAvailability createAvailableResponse() {
            return new MigrationSystemAvailability(true, null);
        }

        public static MigrationSystemAvailability createUnAvailableResponse() {
            return new MigrationSystemAvailability(false, "fail");
        }

        private String getMessageFromException(Throwable th) {
            Throwable rootCause = ExceptionUtils.getRootCause(th);
            return rootCause.getMessage();
        }

        public void setUnavailable() {
            this.avaiable = false;
        }

        public void addCheckResult(String title, CheckResult checkResult) {
            this.checkResults.put(title, checkResult);
        }

        public Map<String, CheckResult> getCheckResults() {
            return checkResults;
        }

        public synchronized void addErrorMessage(String title, Throwable th) {
            setUnavailable();
            addThrowableMessage(title, th);
        }

        public synchronized void addErrorMessage(String title, String log) {
            setUnavailable();
            if(message == null || message.isEmpty()) {
                message = String.format("%s:\n%s", title, log);
            } else {
                message = message + "\n" + title + ":\n" + log;
            }
        }

        public synchronized void addWarningMessage(String log) {
            if(isAvaiable()) {
                this.warning = true;
            }
            if(message == null || message.isEmpty()) {
                message = log;
            } else {
                message = message + "\n" + log;
            }
        }

        public synchronized void addWarningMessage(String title, Throwable th) {
            if(isAvaiable()) {
                this.warning = true;
            }
            addThrowableMessage(title, th);
        }

        private void addThrowableMessage(String title, Throwable th) {
            if(message == null || message.isEmpty()) {
                message = String.format("%s:\n%s", title, getMessageFromException(th));
            } else {
                message = message + "\n" + title + ":\n" + getMessageFromException(th);
            }
        }

        public synchronized boolean isAvaiable() {
            return avaiable;
        }

        public synchronized boolean isWarning() {
            return warning;
        }

        public synchronized String getMessage() {
            return message;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    public static class CheckResult extends RetMessage {

        private long checkTimeMilli = -1L;

        public long getCheckTimeMilli() {
            return checkTimeMilli;
        }

        public void setCheckTimeMilli(long checkTimeMilli) {
            this.checkTimeMilli = checkTimeMilli;
        }

        public CheckResult(int state, String message) {
            super(state, message);
        }

        public CheckResult(int state, String message, long checkTimeMilli) {
            super(state, message);
            this.checkTimeMilli = checkTimeMilli;
        }

        public static CheckResult createFailResult(String message) {
            return new CheckResult(FAIL_STATE, message);
        }

        public static CheckResult createSuccessResult(long checkTimeMilli){
            return createSuccessResult(SUCCESS, checkTimeMilli);
        }

        public static CheckResult createSuccessResult(String message, long checkTimeMilli){
            return new CheckResult(SUCCESS_STATE, message, checkTimeMilli);
        }

        public static CheckResult createWarningResult(String message) {
            return new CheckResult(WARNING_STATE, message);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            CheckResult result = (CheckResult) o;
            return checkTimeMilli == result.checkTimeMilli;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), checkTimeMilli);
        }
    }

}
