package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *         <p>
 *         <p>
 *         sentinel example:
 *         <p>
 *         Jun 19, 2017
 */
public class SentinelHello {

    private HostPort sentinelAddr;
    private HostPort masterAddr;
    private String monitorName;

    public SentinelHello(){
    }

    public SentinelHello(HostPort sentinelAddr, HostPort masterAddr, String monitorName){

        this.sentinelAddr = sentinelAddr;
        this.masterAddr = masterAddr;
        this.monitorName = monitorName;
    }

    /**
     * 10.15.95.133,33322,642d5452b3ffd243fdfc31a2ccf5b0b5963c161f,1942,FlightIntlGDSCacheGroup1,10.15.94.178,6379,0
     *
     * @param helloStr
     * @return
     */
    public static SentinelHello fromString(String helloStr) {

        String[] split = helloStr.split(",");
        if (split.length < 8) {
            throw new IllegalArgumentException("hello not correct:" + helloStr);
        }

        SentinelHello hello = new SentinelHello();
        hello.sentinelAddr = new HostPort(split[0], Integer.parseInt(split[1]));
        hello.masterAddr = new HostPort(split[5], Integer.parseInt(split[6]));
        hello.monitorName = split[4];
        return hello;

    }

    public HostPort getSentinelAddr() {
        return sentinelAddr;
    }

    public HostPort getMasterAddr() {
        return masterAddr;
    }

    public String getMonitorName() {
        return monitorName;
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof SentinelHello)) {
            return false;
        }

        SentinelHello other = (SentinelHello) obj;

        if (!ObjectUtils.equals(sentinelAddr, other.sentinelAddr)) {
            return false;
        }

        if (!ObjectUtils.equals(monitorName, other.monitorName)) {
            return false;
        }

        if (!ObjectUtils.equals(masterAddr, other.masterAddr)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(sentinelAddr, monitorName);
    }

    @Override
    public String toString() {

        return String.format("[stl: %s, master: %s, name:%s]",
                sentinelAddr,
                masterAddr,
                monitorName);
    }
}
