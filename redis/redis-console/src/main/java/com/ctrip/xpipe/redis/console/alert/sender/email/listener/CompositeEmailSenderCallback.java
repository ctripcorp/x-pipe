package com.ctrip.xpipe.redis.console.alert.sender.email.listener;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class CompositeEmailSenderCallback implements AsyncEmailSenderCallback, TopElement {

    private static final Logger logger = LoggerFactory.getLogger(CompositeEmailSenderCallback.class.getSimpleName());

    List<AsyncEmailSenderCallback> callbackFunctions = new LinkedList<>();

    public void register(AsyncEmailSenderCallback callback) {
        for(AsyncEmailSenderCallback function : callbackFunctions) {
            if(function.getClass().getSimpleName().equals(callback.getClass().getSimpleName())) {
                return;
            }
        }
        callbackFunctions.add(callback);
    }

    @Override
    public void success() {
        callbackFunctions.forEach(callbackFunction -> {
            try {
                callbackFunction.success();
            } catch (Exception e) {
                logger.error("[success][{}] exception: {}", callbackFunction.getClass().getSimpleName(), e);
            }
        });
    }

    @Override
    public void fail(Throwable throwable) {
        callbackFunctions.forEach(callbackFunction -> {
            try {
                callbackFunction.fail(throwable);
            } catch (Exception e) {
                logger.error("[fail][{}] exception: {}", callbackFunction.getClass().getSimpleName(), e);
            }
        });
    }

    @VisibleForTesting
    public List<AsyncEmailSenderCallback> getCallbacks() {
        return callbackFunctions;
    }
}
