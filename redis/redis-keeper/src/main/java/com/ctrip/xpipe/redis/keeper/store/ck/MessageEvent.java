package com.ctrip.xpipe.redis.keeper.store.ck;

import java.util.List;

// 1. 定义事件类
public class MessageEvent {
    private List<Object[]> payloads;

    public List<Object[]> getPayloads() {
        return payloads;
    }

    public void setPayloads(List<Object[]> payloads) {
        this.payloads = payloads;
    }
}
