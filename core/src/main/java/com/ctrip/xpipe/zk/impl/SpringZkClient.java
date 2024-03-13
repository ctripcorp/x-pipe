package com.ctrip.xpipe.zk.impl;

import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkConfig;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
public class SpringZkClient implements ZkClient{

    private ZkConfig zkConfig;
    private CuratorFramework curatorFramework;

    public SpringZkClient(ZkConfig zkConfig){
        this.zkConfig = zkConfig;
    }

    @Override
    public void onChange(String key, String val) {
        if (key.equalsIgnoreCase(com.ctrip.xpipe.config.ZkConfig.KEY_ZK_ADDRESS)) {
            this.zkConfig.updateZkAddress(val);
        }
    }

    @PostConstruct
    public void postContruct() throws InterruptedException {
        curatorFramework = zkConfig.create();
    }

    @Override
    public CuratorFramework get() {
        return curatorFramework;
    }

    @Override
    public void setZkAddress(String zkAddress) {
        this.zkConfig.updateZkAddress(zkAddress);
    }

    @Override
    public String getZkAddress() {
        return this.zkConfig.getZkAddress();
    }

    @PreDestroy
    public void preDestroy(){
        curatorFramework.close();
    }
}
