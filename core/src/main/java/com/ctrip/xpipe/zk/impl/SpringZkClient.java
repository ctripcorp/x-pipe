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
    private String zkAddress;
    private CuratorFramework curatorFramework;

    public SpringZkClient(ZkConfig zkConfig, String zkAddress){
        this.zkConfig = zkConfig;
        this.zkAddress = zkAddress;
    }


    @PostConstruct
    public void postContruct() throws InterruptedException {
        curatorFramework = zkConfig.create(zkAddress);
    }

    @Override
    public CuratorFramework get() {
        return curatorFramework;
    }

    @Override
    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    @Override
    public String getZkAddress() {
        return zkAddress;
    }

    @PreDestroy
    public void preDestroy(){
        curatorFramework.close();
    }
}
