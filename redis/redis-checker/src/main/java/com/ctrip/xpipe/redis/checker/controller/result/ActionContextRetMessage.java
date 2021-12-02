package com.ctrip.xpipe.redis.checker.controller.result;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.CheckInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 9:51 PM
 */
public class ActionContextRetMessage<C> extends GenericRetMessage<C> {

    public static <C, I extends CheckInfo, T extends HealthCheckInstance<I>>
    ActionContextRetMessage<C> from(ActionContext<C, T> context) {
        return new ActionContextRetMessage<>(context);
    }

    public static <C, I extends CheckInfo, T extends HealthCheckInstance<I>, A extends ActionContext<C, T>>
    List<ActionContextRetMessage<C>> list(Collection<A> contexts) {
        return contexts.stream().map(ActionContextRetMessage::from).collect(Collectors.toList());
    }

    public static <C, I extends CheckInfo, T extends HealthCheckInstance<I>, A extends ActionContext<C, T>, K>
    Map<K, ActionContextRetMessage<C>> map(Map<K, A> contexts) {
        Map<K, ActionContextRetMessage<C>> result = new HashMap<>();
        for (K k : contexts.keySet()) {
            result.put(k, ActionContextRetMessage.from(contexts.get(k)));
        }
        return result;
    }

    public ActionContextRetMessage() {

    }

    public <I extends CheckInfo, T extends HealthCheckInstance<I>> ActionContextRetMessage(ActionContext<C, T> context) {
        if (context.isSuccess()) {
            setState(SUCCESS_STATE);
            setPayload(context.getResult());
        } else {
            setState(FAIL_STATE);
            setMessage(context.getCause().getMessage());
        }
    }
}
