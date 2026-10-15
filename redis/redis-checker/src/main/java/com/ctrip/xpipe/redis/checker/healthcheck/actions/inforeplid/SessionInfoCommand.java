package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;

/**
 * Adapts {@link RedisSession}'s async INFO to a {@link com.ctrip.xpipe.api.command.Command}, so a
 * check can be composed as a {@code SequenceCommandChain} while the session keeps its callback API
 * (and with it the proxy-aware timeout and this endpoint's connection reuse).
 *
 * The command completes from the session callback, i.e. on whichever thread the INFO response
 * arrived; the chain's executor decides where the next stage runs.
 */
class SessionInfoCommand extends AbstractCommand<String> {

    private final RedisSession session;

    SessionInfoCommand(RedisSession session) {
        this.session = session;
    }

    @Override
    protected void doExecute() {
        session.infoReplication(new Callbackable<String>() {
            @Override
            public void success(String info) {
                future().setSuccess(info);
            }

            @Override
            public void fail(Throwable throwable) {
                future().setFailure(throwable);
            }
        });
    }

    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return "info replication " + session;
    }
}
