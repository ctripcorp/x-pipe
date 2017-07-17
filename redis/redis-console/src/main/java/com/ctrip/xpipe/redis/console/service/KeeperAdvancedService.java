package com.ctrip.xpipe.redis.console.service;

import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public interface KeeperAdvancedService {

    List<KeeperSelected> findBestKeepers(String dcName, int beginPort, BiPredicate<String, Integer> keeperGood);


    public static class KeeperSelected{

        private long keeperContainerId;
        private String host;
        private int port;

        public KeeperSelected(){}

        public KeeperSelected(long keeperContainerId, String host, int port){
            this.keeperContainerId = keeperContainerId;
            this.host = host;
            this.port = port;
        }

        public long getKeeperContainerId() {
            return keeperContainerId;
        }

        public void setKeeperContainerId(long keeperContainerId) {
            this.keeperContainerId = keeperContainerId;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public String toString() {
            return String.format("[%s:%d, keeperContainerId:%d]", host, port, keeperContainerId);
        }
    }

}
