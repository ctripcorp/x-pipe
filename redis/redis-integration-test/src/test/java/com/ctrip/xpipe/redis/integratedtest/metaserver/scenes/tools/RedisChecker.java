package com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.tools;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;

public class RedisChecker {
    XpipeNettyClientKeyedObjectPool pool;
    ScheduledExecutorService scheduled;
    public RedisChecker(XpipeNettyClientKeyedObjectPool pool, ScheduledExecutorService scheduled) {
        this.pool = pool;
        this.scheduled = scheduled;
    }
    static final Logger logger = LoggerFactory.getLogger(ConsoleService.class);
    public BooleanSupplier containsPeer(Endpoint master, Pair<Long, Endpoint> checker) {
        return () -> {
            CRDTInfoCommand crdtInfoCommand = new CRDTInfoCommand(pool.getKeyPool(master), InfoCommand.INFO_TYPE.REPLICATION ,scheduled);
            try {
                CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(crdtInfoCommand.execute().get());
                List<Pair<Long, Endpoint>> peers =  re.extractPeerMasters();
                for(Pair<Long, Endpoint> peer: peers) {
                    if(peer.getKey().equals(checker.getKey()) && peer.getValue().equals(checker.getValue())) {
                        return true;
                    }

                    logger.info("gid: {} , endpoint {}", peer.getKey(), peer.getValue().getProxyProtocol() != null?
                            peer.getValue().getProxyProtocol().getRouteInfo(): peer.getValue().toString());
                }
                return false;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        };
    }
}
