package com.ctrip.xpipe.redis.meta.server.keeper;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public interface KeeperLeaderElectAlgorithm {

	KeeperMeta select(String leaderLatchPath, List<String> children, CuratorFramework curatorFramework) throws Exception;
}
