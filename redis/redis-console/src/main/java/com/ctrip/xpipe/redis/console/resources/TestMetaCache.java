package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestMetaCache implements MetaCache {

    XpipeMeta xpipeMeta = new XpipeMeta();

    public TestMetaCache(){
    }

    @Override
    public XpipeMeta getXpipeMeta() {
        return xpipeMeta;
    }

    @Override
    public boolean inBackupDc(HostPort hostPort) {
        return true;
    }

    @Override
    public HostPort findMasterInSameShard(HostPort hostPort) {
        return null;
    }

    @Override
    public Set<HostPort> allKeepers() {
        return null;
    }

    @Override
    public Pair<String, String> findClusterShard(HostPort hostPort) {
        return null;
    }

    @Override
    public String getSentinelMonitorName(String clusterId, String shardId) {
        return null;
    }

    @Override
    public Set<HostPort> getActiveDcSentinels(String clusterId, String shardId) {
        return null;
    }

    @Override
    public HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException {
        return null;
    }

    @Override
    public String getDc(HostPort hostPort) {
        return null;
    }

    @Override
    public Pair<String, String> findClusterShardBySentinelMonitor(String monitor) {
        return null;
    }

    @Override
    public RouteMeta getRouteIfPossible(HostPort hostPort) {
        return null;
    }

    @Override
    public boolean isReplThroughProxy(String activeDc, String backupDc) {
        return false;
    }

    @Override
    public List<HostPort> getAllRedisOfDc(String dcId) {
        return null;
    }

    public String getActiveDc(String clusterId, String shardId){return null;}

}
