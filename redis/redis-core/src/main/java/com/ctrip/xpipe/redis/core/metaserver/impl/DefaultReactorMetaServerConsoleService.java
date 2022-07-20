package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleService;
import com.ctrip.xpipe.spring.WebClientFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private String dc;

    private String pathChangePrimaryDcCheck;
    private String pathMakeMasterReadOnly;
    private String pathDoChangePrimaryDc;

    private MetricProxy metricProxy = MetricProxy.DEFAULT;

    private Logger logger = LoggerFactory.getLogger(DefaultReactorMetaServerConsoleService.class);

    protected DefaultReactorMetaServerConsoleService(MetaserverAddress metaserverAddress, LoopResources loopResources, ConnectionProvider connectionProvider) {
        this.metaServerClient = WebClientFactory.makeWebClient(loopResources, connectionProvider);
        this.dc = metaserverAddress.getDcName();
        this.pathChangePrimaryDcCheck = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(metaserverAddress.getAddress());
        this.pathMakeMasterReadOnly = META_SERVER_SERVICE.MAKE_MASTER_READONLY.getRealPath(metaserverAddress.getAddress());
        this.pathDoChangePrimaryDc = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(metaserverAddress.getAddress());
    }

    @Override
    public CommandFuture<MetaServerConsoleService.PrimaryDcCheckMessage> changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc) {
        CommandFuture<MetaServerConsoleService.PrimaryDcCheckMessage> future = new DefaultCommandFuture<>();

        long startTime = System.currentTimeMillis();
        String api = "changePrimaryDcCheck";
        metaServerClient.get()
                .uri(this.pathChangePrimaryDcCheck, clusterId, shardId, newPrimaryDc)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(MetaServerConsoleService.PrimaryDcCheckMessage.class)
                .retry(maxRetryTimes)
                .subscribe(res -> {
                    tryMetric(api, dc, clusterId, res.isSuccess(), startTime, System.currentTimeMillis());
                    future.setSuccess(res);
                }, err ->{
                    tryMetric(api, dc, clusterId, false, startTime, System.currentTimeMillis());
                    future.setFailure(err);
                });

        return future;
    }

    @Override
    public CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> makeMasterReadOnly(String clusterId, String shardId, boolean readOnly) {
        CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> future = new DefaultCommandFuture<>();

        long startTime = System.currentTimeMillis();
        String api = "makeMasterReadOnly";
        metaServerClient.put()
                .uri(this.pathMakeMasterReadOnly, clusterId, shardId, readOnly)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(MetaServerConsoleService.PreviousPrimaryDcMessage.class)
                .retry(maxRetryTimes)
                .subscribe(res -> {
                    tryMetric(api, dc, clusterId, true, startTime, System.currentTimeMillis());
                    future.setSuccess(res);
                }, err ->{
                    tryMetric(api, dc, clusterId, false, startTime, System.currentTimeMillis());
                    future.setFailure(err);
                });

        return future;
    }

    @Override
    public CommandFuture<MetaServerConsoleService.PrimaryDcChangeMessage> doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MetaServerConsoleService.PrimaryDcChangeRequest request) {
        CommandFuture<MetaServerConsoleService.PrimaryDcChangeMessage> future = new DefaultCommandFuture<>();

        WebClient.RequestBodySpec bodySpec = metaServerClient.put()
                .uri(this.pathDoChangePrimaryDc, clusterId, shardId, newPrimaryDc);
        WebClient.RequestHeadersSpec<?> headersSpec = null != request ? bodySpec.bodyValue(request): bodySpec;

        long startTime = System.currentTimeMillis();
        String api = "changePrimaryDc";
        headersSpec.accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(MetaServerConsoleService.PrimaryDcChangeMessage.class)
                .retry(maxRetryTimes)
                .subscribe(res -> {
                    tryMetric(api, dc, clusterId, res.isSuccess(), startTime, System.currentTimeMillis());
                    future.setSuccess(res);
                }, err ->{
                    tryMetric(api, dc, clusterId, false, startTime, System.currentTimeMillis());
                    future.setFailure(err);
                });

        return future;
    }

    @VisibleForTesting
    protected void setMetricProxy(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
    }

    private void tryMetric(String api, String dc, String cluster, boolean isSuccess, long startTime, long endTime) {
        try {
            MetricData metricData = new MetricData("call.metaserver", dc, cluster, null);
            metricData.setTimestampMilli(startTime);
            metricData.addTag("api", api);
            metricData.setValue(endTime - startTime);
            metricData.addTag("status", isSuccess ? "SUCCESS" : "FAIL");
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[tryMetric] fail", th);
        }
    }

}
