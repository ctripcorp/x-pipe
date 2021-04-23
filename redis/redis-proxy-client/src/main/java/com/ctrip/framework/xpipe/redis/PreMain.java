package com.ctrip.framework.xpipe.redis;


import com.ctrip.framework.xpipe.redis.agent.ProxyAgent;

import java.lang.instrument.Instrumentation;

public class PreMain {
    public static void premain(String agentOps, Instrumentation inst) {
        inst.addTransformer(new ProxyAgent());
    }
}
