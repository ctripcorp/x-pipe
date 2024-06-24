package com.ctrip.xpipe.redis.proxy.controller;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import com.ctrip.xpipe.spring.AbstractController;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

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

    @Autowired
    private TunnelMonitorManager tunnelMonitorManager;

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

    @RequestMapping(value = "/tunnels/all", method = RequestMethod.GET)
    public String getAllTunnelMetas() {
        List<Tunnel> tunnels = tunnelManager.tunnels();
        List<TunnelMeta> result = Lists.newArrayListWithCapacity(tunnels.size());
        for(Tunnel tunnel : tunnels) {
            result.add(tunnel.getTunnelMeta());
        }
        JsonCodec pretty = new JsonCodec(true);
        return pretty.encode(result);
    }

    @RequestMapping(value = "/tunnels/monitor", method = RequestMethod.GET)
    public String getAllMonitorTunnels() {
        Set<Tunnel> tunnels = tunnelMonitorManager.getAllTunnels();
        List<TunnelMeta> result = Lists.newArrayListWithCapacity(tunnels.size());
        for(Tunnel tunnel : tunnels) {
            result.add(tunnel.getTunnelMeta());
        }
        JsonCodec pretty = new JsonCodec(true);
        return pretty.encode(result);
    }

    @RequestMapping(value = "/tunnel/{id}", method = RequestMethod.GET)
    public String getTunnelMeta(@PathVariable String id) {
        JsonCodec pretty = new JsonCodec(true);
        return pretty.encode(tunnelManager.getById(id).getTunnelMeta());
    }

    @RequestMapping(value = "/tunnel/local/port/{localPort}", method = RequestMethod.DELETE)
    public RetMessage closeTunnel(@PathVariable int localPort) {
        return closeTunnelByLocalPort(localPort);
    }

    private RetMessage closeTunnelByLocalPort(int localPort) {
        Tunnel targetTunnel = null;
        List<Tunnel> tunnels = tunnelManager.tunnels();
        for(Tunnel tunnel : tunnels) {
            if(tunnel == null || tunnel.backend() == null || tunnel.frontend() == null
                    || tunnel.backend().getChannel() == null || tunnel.frontend().getChannel() == null) {
                continue;
            }
            InetSocketAddress address = (InetSocketAddress) tunnel.backend().getChannel().localAddress();
            if(address.getPort() == localPort) {
                targetTunnel = tunnel;
                break;
            }
        }
        if(targetTunnel == null) {
            return RetMessage.createFailMessage("Tunnel not exist for local port: " + localPort);
        }
        tunnelManager.remove(targetTunnel.frontend().getChannel());
        return RetMessage.createSuccessMessage("Tunnel closed for channel: *:" + localPort);
    }

    public static class RetMessage {

        public static final String SUCCESS = "success";

        public static final int WARNING_STATE = 1;
        public static final int SUCCESS_STATE = 0;
        public static final int FAIL_STATE = -1;

        private int state;

        private String message;


        public static RetMessage createFailMessage(String message){
            return new RetMessage(FAIL_STATE, message);
        }

        public static RetMessage createSuccessMessage(){
            return createSuccessMessage(SUCCESS);
        }

        public static RetMessage createSuccessMessage(String message){
            return new RetMessage(SUCCESS_STATE, message);
        }

        public static RetMessage createWarningMessage(String message) {
            return new RetMessage(WARNING_STATE, message);
        }

        public RetMessage(){

        }

        public  RetMessage(int state){
            this.state = state;
        }

        public  RetMessage(int state, String message){

            this.state = state;
            this.message  = message;
        }

        public int getState() {
            return state;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public void setState(int state) {
            this.state = state;
        }
    }
}
