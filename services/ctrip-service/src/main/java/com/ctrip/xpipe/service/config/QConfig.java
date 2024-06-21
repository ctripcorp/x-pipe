package com.ctrip.xpipe.service.config;

import com.ctrip.xpipe.config.AbstractConfig;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qconfig.client.PropertiesChange;
import qunar.tc.qconfig.client.PropertyItem;

import java.util.Map;

public class QConfig extends AbstractConfig {

    public static final String DEFAULT_XPIPE_CONFIG_NAME = "application.properties";

    private MapConfig config;

    public QConfig() {
        this(DEFAULT_XPIPE_CONFIG_NAME);
    }

    public QConfig(String name) {
        config = MapConfig.get(name);
        config.addPropertiesListener(new MapConfig.PropertiesChangeListener() {
            @Override
            public void onChange(PropertiesChange change) {
                for(Map.Entry<String, PropertyItem> entry : change.getItems().entrySet()) {
                    notifyConfigChange(entry.getKey(), entry.getValue().getOldValue(), entry.getValue().getNewValue());
                }
            }
        });
    }

    @Override
    public String get(String key) {
        return config.asMap().get(key);
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
