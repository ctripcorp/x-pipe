package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * keepercontainer 注册到 ZK 的节点数据，格式为 {"IpAddress": "10.0.0.5"}。
 * <p>
 * 属性名首字母大写是既定的节点数据契约，Jackson 默认会按 getter 推导出首字母小写的
 * ipAddress，因此必须显式声明 JsonProperty 固定为 IpAddress。
 */
public class KeeperContainerAddressInfo {

    @JsonProperty("IpAddress")
    private String ipAddress;

    public KeeperContainerAddressInfo() {
    }

    public KeeperContainerAddressInfo(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @JsonProperty("IpAddress")
    public String getIpAddress() {
        return ipAddress;
    }

    @JsonProperty("IpAddress")
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @Override
    public String toString() {
        return Codec.DEFAULT.encode(this);
    }
}
