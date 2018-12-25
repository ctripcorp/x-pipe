package com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration;

import com.ctrip.xpipe.exception.ExceptionUtils;

public interface MigrationSystemAvailableChecker {

    MigrationSystemAvailability getResult();

    public static class MigrationSystemAvailability {

        private boolean avaiable;

        private boolean warning = false;

        private long timestamp = System.currentTimeMillis();

        private String message;

        public MigrationSystemAvailability(boolean avaiable, String message) {
            this.avaiable = avaiable;
            this.message = message;
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

        public synchronized void addErrorMessage(String title, Throwable th) {
            setUnavailable();
            if(message == null || message.isEmpty()) {
                message = String.format("%s\n%s", title, getMessageFromException(th));
            } else {
                StringBuilder sb = new StringBuilder(message);
                sb.append("\n").append(title).append("\n").append(getMessageFromException(th));
                message = sb.toString();
            }
        }

        public synchronized void addErrorMessage(String title, String log) {
            setUnavailable();
            if(message == null || message.isEmpty()) {
                message = String.format("%s\n%s", title, log);
            } else {
                StringBuilder sb = new StringBuilder(message);
                sb.append("\n").append(title).append("\n").append(log);
                message = sb.toString();
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
}
