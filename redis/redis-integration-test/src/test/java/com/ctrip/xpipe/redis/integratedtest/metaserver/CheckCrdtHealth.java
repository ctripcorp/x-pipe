package com.ctrip.xpipe.redis.integratedtest.metaserver;

import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.util.Map;
import java.util.Set;

public class CheckCrdtHealth {
    Logger logger = LoggerFactory.getLogger(CheckCrdtHealth.class);
    protected RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(3, 100);
    String console_url;
    String idc;
    String clustername;
    String shardname;
    String url ;
    public CheckCrdtHealth(String console_url, String idc, String clustername, String shardname) {
        this.console_url = console_url;
        this.idc = idc;
        this.clustername = clustername;
        this.shardname = shardname;
        updateUrl();
    }
    public void updateUrl() {
        this.url = String.format("http://%s/console/cross-master/delay/bi_direction/%s/%s/%s", this.console_url, this.idc, this.clustername, this.shardname);
    }

    public CheckCrdtHealth setConsole_url(String console_url) {
        this.console_url = console_url;
        updateUrl();
        return this;
    }

    public boolean checkConsoleHealth() {
        try {
            String rst = restTemplate.getForObject(this.url, String.class);

            if(rst == null  || rst.equals("{}")) {
                return false;
            }
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(rst);
            JsonObject obj = element.getAsJsonObject();
            if(obj == null || obj.entrySet().size() == 0) {
                return false;
            }
            for( Map.Entry<String, JsonElement> entrys : obj.entrySet()) {
                JsonObject o = entrys.getValue().getAsJsonObject();
                for(Map.Entry<String, JsonElement> es: o.entrySet()) {
                    int r = es.getValue().getAsInt();
                    if( r < 0 && r > 10000) {
                        return false;
                    }
                }
            }

            logger.info("[checkConsoleHealth] rst {}", rst);

            return true;
        } catch (Exception e) {
            logger.info("[checkConsoleHealth] check fail", e);
            return false;
        }
    }
}