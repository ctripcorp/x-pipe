package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.*;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public interface CheckerConsoleService {

    XpipeMeta getXpipeMeta(String console, int clusterPartIndex) throws SAXException, IOException;

    XpipeMeta getXpipeAllMeta(String console) throws SAXException, IOException;

    XpipeMeta getXpipeAllDCMeta(String console, String dcName) throws SAXException, IOException;
    
    List<ProxyTunnelInfo> getProxyTunnelInfos(String console);

    void ack(String console, CheckerStatus checkerStatus);

    void report(String console, HealthCheckResult result);
    
    boolean isClusterOnMigration(String console, String clusterId);

    void updateRedisRole(String console, RedisHealthCheckInstance instance, Server.SERVER_ROLE role);
    
    Set<String> sentinelCheckWhiteList(String console);

    Set<String> clusterAlertWhiteList(String console);

    Set<String> migratingClusterList(String console);

    boolean isSentinelAutoProcess(String console);

    boolean isAlertSystemOn(String console);

    Date getClusterCreateTime(String console, String clusterId);
    
    Map<String, Date> loadAllClusterCreateTime(String console);

    Map<String, OuterClientService.ClusterInfo> loadAllActiveDcOneWayClusterInfo(String console, String activeDc);

    void bindShardSentinel(String console, String dc, String cluster, String shard, SentinelMeta sentinelMeta);

    public class AlertMessage {
        private AlertMessageEntity message;
        private Properties properties;
        private String eventOperator;

        public AlertMessage() {

        }
        public AlertMessage(String eventOperator, AlertMessageEntity message, EmailResponse response) {
            this.message = message;
            this.properties = response.getProperties();
            this.eventOperator = eventOperator;
        }

        public AlertMessageEntity getMessage() {
            return message;
        }
        @JsonIgnore
        public EmailResponse getEmailResponse() {
            return new EmailResponse() {
                @Override
                public Properties getProperties() {
                    return properties;
                }
            };
        }

        public void setMessage(AlertMessageEntity message) {
            this.message = message;
        }

        public void setProperties(Properties properties) {
            this.properties = properties;
        }

        public Properties getProperties() {
            return properties;
        }

        public void setEventOperator(String eventOperator) {
            this.eventOperator = eventOperator;
        }

        public String getEventOperator() {
            return eventOperator;
        }
    }
    
    void recordAlert(String console, String eventOperator, AlertMessageEntity message, EmailResponse response);
}
