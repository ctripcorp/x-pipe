package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2021/11/30
 */
public class MasterReplState {

    public enum MASTER_REPL_STATE {

        REPL_NONE,
        REPL_CONNECTING,
        REPL_CONNECTED,
        REPL_DISCONNECTED,
        REPL_STOPPING,
        REPL_STOPPED

    }

    private MASTER_REPL_STATE state = MASTER_REPL_STATE.REPL_NONE;

    private CommandFuture<Void> connectedFuture = new DefaultCommandFuture<>();

    private CommandFuture<Void> stoppedFuture = new DefaultCommandFuture<>();

    private static final Logger logger = LoggerFactory.getLogger(MasterReplState.class);

    public CommandFuture<Void> waitReplStopped() {
        return stoppedFuture;
    }

    public CommandFuture<Void> waitReplConnected() {
        return connectedFuture;
    }

    public MASTER_REPL_STATE getState() {
        return state;
    }

    public synchronized void startConnect() {
        if (MASTER_REPL_STATE.REPL_NONE == state || MASTER_REPL_STATE.REPL_DISCONNECTED == state
                || MASTER_REPL_STATE.REPL_STOPPING == state || MASTER_REPL_STATE.REPL_STOPPED == state) {
            logger.info("[startConnect] {} -> {}", state, MASTER_REPL_STATE.REPL_CONNECTING);
            if (stoppedFuture.isDone()) this.stoppedFuture = new DefaultCommandFuture<>();
            if (connectedFuture.isDone()) this.connectedFuture = new DefaultCommandFuture<>();
            state = MASTER_REPL_STATE.REPL_CONNECTING;
        } else {
            logger.info("[startConnect][unexpected current state] {}", state);
        }
    }

    public synchronized void connectFail() {
        if (MASTER_REPL_STATE.REPL_CONNECTING == state) {
            logger.info("[connectFail] {} -> {}", state, MASTER_REPL_STATE.REPL_DISCONNECTED);
            this.state = MASTER_REPL_STATE.REPL_DISCONNECTED;
        } else if (MASTER_REPL_STATE.REPL_STOPPING == state) {
            logger.info("[connectFail] {} -> {}", state, MASTER_REPL_STATE.REPL_STOPPED);
            this.state = MASTER_REPL_STATE.REPL_STOPPED;
            if (!this.stoppedFuture.isDone()) this.stoppedFuture.setSuccess();
        } else {
            logger.info("[connectFail][unexpected current state] {}", state);
        }
    }

    public synchronized void connected() {
        if (MASTER_REPL_STATE.REPL_CONNECTING == state) {
            logger.info("[connected] {} -> {}", state, MASTER_REPL_STATE.REPL_CONNECTED);
            state = MASTER_REPL_STATE.REPL_CONNECTED;
            if (!this.connectedFuture.isDone()) this.connectedFuture.setSuccess();
        } else if (MASTER_REPL_STATE.REPL_STOPPING == state) {
            logger.info("[connected] remain {}", MASTER_REPL_STATE.REPL_STOPPING);
            state = MASTER_REPL_STATE.REPL_STOPPING;
        } else {
            logger.info("[connected][unexpected current state] {}", state);
        }
    }

    public synchronized void disconnected() {
        if (MASTER_REPL_STATE.REPL_CONNECTED == state) {
            logger.info("[disconnected] {} -> {}", state, MASTER_REPL_STATE.REPL_DISCONNECTED);
            this.state = MASTER_REPL_STATE.REPL_DISCONNECTED;
        } else if (MASTER_REPL_STATE.REPL_STOPPING == state) {
            logger.info("[disconnected] {} -> {}", state, MASTER_REPL_STATE.REPL_STOPPED);
            this.state = MASTER_REPL_STATE.REPL_STOPPED;
            if (!this.stoppedFuture.isDone()) this.stoppedFuture.setSuccess();
        } else {
            logger.info("[disconnected][unexpected current state] {}", state);
        }
    }

    public synchronized void stopConnect() {
        if (MASTER_REPL_STATE.REPL_CONNECTED == state || MASTER_REPL_STATE.REPL_CONNECTING == state) {
            logger.info("[stopConnect] {} -> {}", state, MASTER_REPL_STATE.REPL_STOPPING);
            this.state = MASTER_REPL_STATE.REPL_STOPPING;
        } else if (MASTER_REPL_STATE.REPL_NONE == state || MASTER_REPL_STATE.REPL_DISCONNECTED == state) {
            logger.info("[stopConnect] {} -> {}", state, MASTER_REPL_STATE.REPL_STOPPED);
            this.state = MASTER_REPL_STATE.REPL_STOPPED;
            if (!this.stoppedFuture.isDone()) this.stoppedFuture.setSuccess();
        } else {
            logger.info("[stopConnect][unexpected current state] {}", state);
        }
    }

}
