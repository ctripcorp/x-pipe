package com.ctrip.xpipe.config;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
public interface ZkConfig {

    String KEY_ZK_ADDRESS  = "zk.address";
    String KEY_ZK_NAMESPACE  = "zk.namespace";

    String getZkConnectionString();

    String getZkNameSpace();

}
