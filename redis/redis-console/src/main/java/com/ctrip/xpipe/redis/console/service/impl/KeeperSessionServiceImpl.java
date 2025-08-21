package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.KeeperStateModel;
import com.ctrip.xpipe.redis.console.healthcheck.session.KeeperConsoleSessionManager;
import com.ctrip.xpipe.redis.console.keeper.command.KeeperReplIdGetCommand;
import com.ctrip.xpipe.redis.console.service.KeeperSessionService;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Component
public class KeeperSessionServiceImpl extends AbstractService implements KeeperSessionService {

    @Autowired
    private KeeperConsoleSessionManager sessionManager;

    @Autowired
    private MetaCache metaCache;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    @Override
    public CommandFuture<String> infoKeeper(String ip, int port, String infoSection, Callbackable<String> callback) {
        return sessionManager.findOrCreateSession(new DefaultEndPoint(ip, port)).info(infoSection,callback);
    }

    @Override
    public CommandFuture<Role> getKeeperRole(String ip, int port, RedisSession.RollCallback callback) {
        return sessionManager.findOrCreateSession(new DefaultEndPoint(ip, port)).role(callback);
    }

    @Override
    public KeeperInstanceMeta getKeeperReplId(String ip, int port) {
        return restTemplate.getForObject(getPath(ip, 8080, PATH_KEEPER_INFO_PORT), KeeperInstanceMeta.class, port);
    }

    @Override
    public List<KeeperStateModel> getShardAllKeeperState(String dcId, String clusterId, String shardId) {
        List<KeeperMeta> redisOfDcClusterShard = metaCache.getKeeperOfDcClusterShard(dcId, clusterId, shardId);
        Map<HostPort, KeeperStateModel> resultMap = new HashMap<>();
        Map<HostPort, List<CommandFuture<?>>> futuresMap = new HashMap<>();
        redisOfDcClusterShard.forEach(keeperMeta -> {
            HostPort keeper = new HostPort(keeperMeta.getIp(), keeperMeta.getPort());
            List<CommandFuture<?>> futures = new ArrayList<>();
            futuresMap.put(keeper, futures);
            KeeperStateModel model = new KeeperStateModel().setHost(keeperMeta.getIp()).setPort(keeperMeta.getPort());
            resultMap.put(keeper, model);
            CommandFuture<Role> keeperRole = getKeeperRole(keeperMeta.getIp(), keeperMeta.getPort(), new RedisSession.RollCallback() {
                @Override
                public void role(String role, Role detail) {
                    model.setRole(((SlaveRole) detail).getMasterState());
                }

                @Override
                public void fail(Throwable e) {
                    model.addErr("getKeeperRole", e);
                }
            });
            futures.add(keeperRole);
            CommandFuture<String> keeperInfo = infoKeeper(keeperMeta.getIp(), keeperMeta.getPort(), InfoCommand.INFO_TYPE.REPLICATION.cmd(), new Callbackable<String>() {
                @Override
                public void success(String message) {
                    InfoResultExtractor extractor = new InfoResultExtractor(message);
                    model.setState(extractor.getKeeperState())
                            .setMasterHost(extractor.getKeyKeeperMasterHost())
                            .setMasterPort(extractor.getKeyKeeperMasterPort())
                            .setMasterReplOffset(extractor.getMasterReplOffset())
                            .setReplBacklogSize(extractor.getKeyKeeperReplBacklogSize())
                            .setSlavesMap(extractor.getKeyKeeperSlaves());
                }

                @Override
                public void fail(Throwable e) {
                    model.addErr("infoKeeper", e);
                }
            });
            futures.add(keeperInfo);
            CommandFuture<Long> keeperRepl = new KeeperReplIdGetCommand(this, keeper).execute(executor);
            keeperRepl.addListener(commandFuture -> {
                if (commandFuture.isSuccess()) {
                    model.setReplId(commandFuture.get());
                } else {
                    model.addErr("getKeeperReplId", commandFuture.cause());
                }
            });
            futures.add(keeperRepl);
        });

        for (Map.Entry<HostPort, List<CommandFuture<?>>> entry : futuresMap.entrySet()) {
            entry.getValue().forEach(commandFuture -> {
                try {
                    commandFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    resultMap.get(entry.getKey()).addErr("commandFutureExecute", e);
                }
            });
        }

        return new ArrayList<>(resultMap.values());
    }

    private String getPath(String ip, int port, String path) {
        return "http://" + ip + ":" + port + path;
    }
}
