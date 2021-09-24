package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleService;
import com.ctrip.xpipe.spring.WebClientFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

/**
 * @author lishanglin
 * date 2021/9/23
 */
public class DefaultReactorMetaServerConsoleService implements ReactorMetaServerConsoleService {

    private static final int maxRetryTimes = Integer.parseInt(System.getProperty("http.retry-times", "1"));

    private WebClient metaServerClient;

    private String pathChangePrimaryDcCheck;
    private String pathMakeMasterReadOnly;
    private String pathDoChangePrimaryDc;

    protected DefaultReactorMetaServerConsoleService(String metaServerAddress, LoopResources loopResources, ConnectionProvider connectionProvider) {
        this.metaServerClient = WebClientFactory.makeWebClient(loopResources, connectionProvider);
        this.pathChangePrimaryDcCheck = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(metaServerAddress);
        this.pathMakeMasterReadOnly = META_SERVER_SERVICE.MAKE_MASTER_READONLY.getRealPath(metaServerAddress);
        this.pathDoChangePrimaryDc = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(metaServerAddress);
    }

    @Override
    public CommandFuture<MetaServerConsoleService.PrimaryDcCheckMessage> changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc) {
        CommandFuture<MetaServerConsoleService.PrimaryDcCheckMessage> future = new DefaultCommandFuture<>();

        metaServerClient.get()
                .uri(this.pathChangePrimaryDcCheck, clusterId, shardId, newPrimaryDc)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(MetaServerConsoleService.PrimaryDcCheckMessage.class)
                .retry(maxRetryTimes)
                .subscribe(future::setSuccess, future::setFailure);

        return future;
    }

    @Override
    public CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> makeMasterReadOnly(String clusterId, String shardId, boolean readOnly) {
        CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> future = new DefaultCommandFuture<>();

        metaServerClient.put()
                .uri(this.pathMakeMasterReadOnly, clusterId, shardId, readOnly)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(MetaServerConsoleService.PreviousPrimaryDcMessage.class)
                .retry(maxRetryTimes)
                .subscribe(future::setSuccess, future::setFailure);

        return future;
    }

    @Override
    public CommandFuture<MetaServerConsoleService.PrimaryDcChangeMessage> doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MetaServerConsoleService.PrimaryDcChangeRequest request) {
        CommandFuture<MetaServerConsoleService.PrimaryDcChangeMessage> future = new DefaultCommandFuture<>();

        WebClient.RequestBodySpec bodySpec = metaServerClient.put()
                .uri(this.pathDoChangePrimaryDc, clusterId, shardId, newPrimaryDc);
        WebClient.RequestHeadersSpec<?> headersSpec = null != request ? bodySpec.bodyValue(request): bodySpec;

        headersSpec.accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(MetaServerConsoleService.PrimaryDcChangeMessage.class)
                .retry(maxRetryTimes)
                .subscribe(future::setSuccess, future::setFailure);

        return future;
    }
}
