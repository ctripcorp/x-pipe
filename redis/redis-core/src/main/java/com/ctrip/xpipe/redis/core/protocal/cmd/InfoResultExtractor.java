package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 22, 2018
 */
public class InfoResultExtractor {

    protected static Logger logger = LoggerFactory.getLogger(InfoResultExtractor.class);

    private String result;
    private Map<String, String> keyValues;

    public InfoResultExtractor(String result) {
        this.result = result;
    }

    public String extract(String key) {
        return extract(key, (value) -> value);
    }

    public <T> T extract(String key, Function<String, T> function) {
        genKeyValues();

        return function.apply(keyValues.get(key));
    }

    public Integer extractAsInteger(String key) {

        return extract(key, (value) -> value == null ? null : Integer.parseInt(value));
    }

    public Long extractAsLong(String key) {
        return extract(key, (value) -> value == null ? null : Long.parseLong(value));
    }

    public Map<String, String> extract(String[] keys) {

        genKeyValues();

        HashMap<String, String> result = new HashMap<>();
        for (String key : keys) {
            result.put(key, keyValues.get(key));
        }
        return result;
    }

    private void genKeyValues() {

        if (keyValues == null) {
            synchronized (this) {
                if (keyValues == null) {
                    keyValues = new HashMap<>();
                    String[] split = result.split("[\r\n]+");
                    for (String line : split) {
                        if(line == null || line.isEmpty()) {
                            continue;
                        }
                        if (line.startsWith("#")) {
                            continue;
                        }
                        int splitterIndex = line.indexOf(":");
                        if (splitterIndex < 0 || splitterIndex >= line.length()) {
                            logger.warn("[wrong format]{}", line);
                            continue;
                        }

                        keyValues.put(line.substring(0, splitterIndex).trim(),
                                line.substring(splitterIndex + 1).trim());
                    }
                }
            }
        }

    }

}
