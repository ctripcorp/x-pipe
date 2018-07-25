package com.ctrip.xpipe.redis.proxy.controller;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import com.ctrip.xpipe.spring.AbstractController;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 30, 2018
 */
@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ProxyController {

    @Autowired
    private TunnelManager tunnelManager;

    @RequestMapping(value = "/tunnels", method = RequestMethod.GET)
    public String getTunnelMetas() {
        List<Tunnel> tunnels = tunnelManager.tunnels();
        List<TunnelMeta> result = Lists.newArrayListWithCapacity(tunnels.size());
        for(Tunnel tunnel : tunnels) {
            if(tunnel.getState() instanceof TunnelEstablished) {
                result.add(tunnel.getTunnelMeta());
            }
        }
        JsonCodec pretty = new JsonCodec(true);
        return pretty.encode(result);
    }

    @RequestMapping(value = "/tunnel/{id}", method = RequestMethod.GET)
    public String getTunnelMeta(@PathVariable String id) {
        JsonCodec pretty = new JsonCodec(true);
        return pretty.encode(tunnelManager.getById(id).getTunnelMeta());
    }
}
