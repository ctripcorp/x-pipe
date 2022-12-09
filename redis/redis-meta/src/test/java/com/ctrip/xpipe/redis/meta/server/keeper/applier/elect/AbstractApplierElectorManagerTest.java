package com.ctrip.xpipe.redis.meta.server.keeper.applier.elect;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.cluster.DefaultLeaderElector;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author ayq
 * <p>
 * 2022/4/17 21:30
 */
public class AbstractApplierElectorManagerTest extends AbstractMetaServerContextTest {

    protected LeaderElector addApplierZkNode(String clusterId, String shardId, ZkClient zkClient) throws Exception {

        return addApplierZkNode(clusterId, shardId, zkClient, 0);

    }

    protected LeaderElector addApplierZkNode(Long clusterDbId, Long shardDbId, ZkClient zkClient) throws Exception {

        return addApplierZkNode(clusterDbId, shardDbId, zkClient, 0);

    }

    protected LeaderElector addApplierZkNode(String clusterId, String shardId, ZkClient zkClient, int idLen) throws Exception {
        if (0 == idLen) return addApplierZkNode(clusterId, shardId, zkClient, new ApplierMeta());
        else return addApplierZkNode(clusterId, shardId, zkClient, new ApplierMeta().setId(randomString(idLen)));
    }

    protected LeaderElector addApplierZkNode(Long clusterDbId, Long shardDbId, ZkClient zkClient, int idLen) throws Exception {
        if (0 == idLen) return addApplierZkNode(clusterDbId, shardDbId, zkClient, new ApplierMeta());
        else return addApplierZkNode(clusterDbId, shardDbId, zkClient, new ApplierMeta().setId(randomString(idLen)));
    }

    protected LeaderElector addApplierZkNode(String clusterId, String shardId, ZkClient zkClient, ApplierMeta applierMeta) throws Exception {
        String leaderElectionZKPath = MetaZkConfig.getApplierLeaderLatchPath(clusterId, shardId);
        String leaderElectionID = MetaZkConfig.getApplierLeaderElectionId(applierMeta);
        return addApplierZkNode(zkClient, leaderElectionID, leaderElectionZKPath);
    }

    protected LeaderElector addApplierZkNode(Long clusterDbId, Long shardDbId, ZkClient zkClient, ApplierMeta applierMeta) throws Exception {
        String leaderElectionZKPath = MetaZkConfig.getApplierLeaderLatchPath(clusterDbId, shardDbId);
        String leaderElectionID = MetaZkConfig.getApplierLeaderElectionId(applierMeta);
        return addApplierZkNode(zkClient, leaderElectionID, leaderElectionZKPath);
    }

    protected LeaderElector addApplierZkNode(ZkClient zkClient, String leaderElectionID, String leaderElectionZKPath) throws Exception {
        ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
        LeaderElector leaderElector = new DefaultLeaderElector(ctx, zkClient.get());
        leaderElector.initialize();
        leaderElector.start();

        return leaderElector;
    }
}
