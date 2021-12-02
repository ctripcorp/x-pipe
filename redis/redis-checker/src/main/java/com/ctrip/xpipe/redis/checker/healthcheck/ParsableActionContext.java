package com.ctrip.xpipe.redis.checker.healthcheck;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 10:24 PM
 */
public interface ParsableActionContext<P, C, T extends HealthCheckInstance> extends ActionContext<P, T> {

    ActionContext<C, T> inner();

    P parse(C c);

    @Override
    default P getResult() {
        return parse(inner().getResult());
    }

    @Override
    default T instance() {
        return inner().instance();
    }

    @Override
    default long getRecvTimeMilli() {
        return inner().getRecvTimeMilli();
    }

    @Override
    default boolean isSuccess() {
        return inner().isSuccess();
    }

    @Override
    default Throwable getCause() {
        return inner().getCause();
    }
}
