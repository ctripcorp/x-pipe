package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.sun.org.apache.bcel.internal.generic.DCMPG;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
@Component
@Lazy
public class DefaultMetaCache implements  MetaCache{

    private int loadIntervalSeconds = 30;

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private DcService dcService;

    private List<DcMeta> dcMetas;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    public DefaultMetaCache(){
    }

    @PostConstruct
    public void postConstruct(){

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                loadCache();
            }
        }, 1, loadIntervalSeconds, TimeUnit.SECONDS);
    }

    private void loadCache() {

        List<DcTbl> dcs = dcService.findAllDcNames();
        List<DcMeta> dcMetas = new LinkedList<>();
        for (DcTbl dc : dcs) {
            dcMetas.add(dcMetaService.getDcMeta(dc.getDcName()));
        }
        this.dcMetas = dcMetas;
    }

    @Override
    public List<DcMeta> getDcMetas() {
        return dcMetas;
    }

    @Override
    public XpipeMeta getXpipeMeta() {

        XpipeMeta xpipeMeta = new XpipeMeta();
        for(DcMeta dcMeta : dcMetas){
            xpipeMeta.addDc(dcMeta);
        }
        return xpipeMeta;
    }

    @Override
    public boolean inBackupDc(HostPort hostPort) {

        XpipeMeta xpipeMeta = getXpipeMeta();

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(xpipeMeta);
        ShardMeta shardMeta = xpipeMetaManager.findShardMeta(hostPort);
        if(shardMeta == null){
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }
        String instanceInDc = shardMeta.parent().parent().getId();
        String activeDc = shardMeta.getActiveDc();
        return !activeDc.equalsIgnoreCase(instanceInDc);
    }

    @Override
    public HostPort findMasterInSameShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = new DefaultXpipeMetaManager(getXpipeMeta());

        ShardMeta currentShard = xpipeMetaManager.findShardMeta(hostPort);
        if(currentShard == null){
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String clusterName = currentShard.parent().getId();
        String shardName = currentShard.getId();

        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterName, shardName);
        RedisMeta redisMeta = redisMaster.getValue();
        return new HostPort(redisMeta.getIp(), redisMeta.getPort());
    }

}
