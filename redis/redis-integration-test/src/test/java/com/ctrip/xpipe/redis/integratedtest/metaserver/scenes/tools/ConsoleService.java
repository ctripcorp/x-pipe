package com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.tools;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.tuple.Pair;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;

public class ConsoleService {
    private String consoleServerUrl;
    private String idc;
    private RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(3, 100);

    public ConsoleService(String idc, String url) {
        this.idc = idc;
        this.consoleServerUrl = url;
    }


    public void changeRoute(RouteModel model) {
        restTemplate.put(String.format("http://%s/api/route", consoleServerUrl), model);
    }

    public void delRoute(RouteModel model) {
        restTemplate.delete(String.format("http://%s/api/route", consoleServerUrl), model);
    }

    BooleanSupplier waitShardHealthTask(String clustername, String shardname) {
        String url = String.format("http://%s/console/cross-master/delay/bi_direction/%s/%s/%s", this.consoleServerUrl, this.idc, clustername, shardname);
        return () -> {
            String rest = restTemplate.getForObject(url, String.class);
            if (rest == null || rest.equals("{}")) {
                return false;
            }
            JsonObject obj = new JsonParser().parse(rest).getAsJsonObject();
            if (obj == null || obj.entrySet().size() == 0) {
                return false;
            }
            for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                JsonObject o = entry.getValue().getAsJsonObject();
                for (Map.Entry<String, JsonElement> es : o.entrySet()) {
                    int r = es.getValue().getAsInt();
                    if (r < 0 || r > 10000) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

}