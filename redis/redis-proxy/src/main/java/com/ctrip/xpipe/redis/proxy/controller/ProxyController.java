package com.ctrip.xpipe.redis.proxy.controller;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

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
    public List<TunnelMeta> getTunnelMetas() {
        List<Tunnel> tunnels = tunnelManager.tunnels();
        return tunnels.stream().map(Tunnel::getTunnelMeta).collect(Collectors.toList());
    }

    @RequestMapping(value = "/tunnel/{id}", method = RequestMethod.GET)
    public TunnelMeta getTunnelMeta(@PathVariable String id) {
        return tunnelManager.getById(id).getTunnelMeta();
    }
}
