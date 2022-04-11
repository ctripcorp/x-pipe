package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.pojo.AbstractInfo;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MasterSlavesInfo extends AbstractInfo {

    private static final String CONNECTED_SLAVES_PREFIX = "connected_slaves";
    private static final String SLAVE_PREFIX = "slave";
    private static final String SLAVE_IP_PREFIX = "ip";
    private static final String SLAVE_PORT_PREFIX = "port";

    private List<HostPort> slaves;

    public List<HostPort> getSlaves() {
        return slaves;
    }

    public MasterSlavesInfo(List<HostPort> slaves) {
        super(Server.SERVER_ROLE.MASTER,false);
        this.slaves = slaves;
    }

    public static MasterSlavesInfo fromInfo(InfoResultExtractor extractor){

        int connectedSlavesCount = extractor.extractAsInteger(CONNECTED_SLAVES_PREFIX);
        List<HostPort> slaves = new ArrayList<>();
        for (int i = 0; i < connectedSlavesCount; i++) {
            HostPort slave = extractor.extract(SLAVE_PREFIX + i, input -> {
                if (Strings.isNullOrEmpty(input))
                    return null;

                Map<String, String> slaveInfoMap = new HashMap<>();
                String[] slaveInfos = input.split("\\s*,\\s*");
                for (String slaveInfo : slaveInfos) {
                    String[] slaveElements = slaveInfo.split("\\s*=\\s*");
                    if (slaveElements.length != 2)
                        continue;
                    slaveInfoMap.put(slaveElements[0], slaveElements[1]);
                }
                String ip = slaveInfoMap.get(SLAVE_IP_PREFIX);
                String port = slaveInfoMap.get(SLAVE_PORT_PREFIX);
                if (ip != null && port != null)
                    return new HostPort(ip, Integer.parseInt(port));

                return null;
            });
            if (slave != null)
                slaves.add(slave);
        }

        return new MasterSlavesInfo(slaves);
    }
}
