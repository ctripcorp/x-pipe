package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.cluster.DefaultLeaderElector;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author lishanglin
 * date 2021/12/4
 */
public class AbstractKeeperElectorManagerTest extends AbstractMetaServerContextTest {

    protected LeaderElector addKeeperZkNode(String clusterId, String shardId, ZkClient zkClient) throws Exception {

        return addKeeperZkNode(clusterId, shardId, zkClient, 0);

    }

    protected LeaderElector addKeeperZkNode(Long clusterDbId, Long shardDbId, ZkClient zkClient) throws Exception {

        return addKeeperZkNode(clusterDbId, shardDbId, zkClient, 0);

    }

    protected LeaderElector addKeeperZkNode(String clusterId, String shardId, ZkClient zkClient, int idLen) throws Exception {
        if (0 == idLen) return addKeeperZkNode(clusterId, shardId, zkClient, new KeeperMeta());
        else return addKeeperZkNode(clusterId, shardId, zkClient, new KeeperMeta().setId(randomString(idLen)));
    }

    protected LeaderElector addKeeperZkNode(Long clusterDbId, Long shardDbId, ZkClient zkClient, int idLen) throws Exception {
        if (0 == idLen) return addKeeperZkNode(clusterDbId, shardDbId, zkClient, new KeeperMeta());
        else return addKeeperZkNode(clusterDbId, shardDbId, zkClient, new KeeperMeta().setId(randomString(idLen)));
    }

    protected LeaderElector addKeeperZkNode(String clusterId, String shardId, ZkClient zkClient, KeeperMeta keeperMeta) throws Exception {
        String leaderElectionZKPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
        String leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(keeperMeta);
        return addKeeperZkNode(zkClient, leaderElectionID, leaderElectionZKPath);
    }

    protected LeaderElector addKeeperZkNode(Long clusterDbId, Long shardDbId, ZkClient zkClient, KeeperMeta keeperMeta) throws Exception {
        String leaderElectionZKPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterDbId, shardDbId);
        String leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(keeperMeta);
        return addKeeperZkNode(zkClient, leaderElectionID, leaderElectionZKPath);
    }

    protected LeaderElector addKeeperZkNode(ZkClient zkClient, String leaderElectionID, String leaderElectionZKPath) throws Exception {
        ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
        LeaderElector leaderElector = new DefaultLeaderElector(ctx, zkClient.get());
        leaderElector.initialize();
        leaderElector.start();

        return leaderElector;
    }
    
}
