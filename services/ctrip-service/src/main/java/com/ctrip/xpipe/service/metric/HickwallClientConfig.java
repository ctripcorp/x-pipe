package com.ctrip.xpipe.service.metric;

/**
 * @author chen.zhu
 * <p>
 * May 20, 2020
 */
public class HickwallClientConfig {
    public String PROXY_ADDRESS;
    public Integer BATCH_SIZE = 100;
    public Integer BUFFER_SIZE = 1000;
    public Integer CONNECTION_TIMEOUT_MS = 1000;
    public Integer BUFFER_TIMEOUT_MS = 1000;
    public Integer READ_TIMEOUT_MS = 1000;
    public Integer RETRIES = 1;
    public Integer THREAD_NUM = 1;

    public HickwallClientConfig(String PROXY_ADDRESS) {
        this.PROXY_ADDRESS = PROXY_ADDRESS;
    }
}
