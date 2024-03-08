package com.ctrip.xpipe.config;

/**
 * @author lishanglin
 * date 2024/3/8
 */
public interface ConfigKeyListener {

    void onChange(String key, String val);

}
