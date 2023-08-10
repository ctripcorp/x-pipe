package com.ctrip.xpipe.redis.core.protocal.cmd;

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

    private static final String KEY_SYNC_FULL = "sync_full";
    private static final String KEY_SYNC_PARTIAL_OK = "sync_partial_ok";
    private static final String KEY_SYNC_PARTIAL_ERR = "sync_partial_err";

    private static final String KEY_MASTER_REPL_OFFSET = "master_repl_offset";
    private static final String KEY_SLAVE_REPL_OFFSET = "slave_repl_offset";

    private static final String KEY_INSTANTANEOUS_INPUT_KBPS = "instantaneous_input_kbps";

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
                    Map<String, String> localMap = new HashMap<>();
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

                        localMap.put(line.substring(0, splitterIndex).trim(),
                                line.substring(splitterIndex + 1).trim());
                    }
                    keyValues = localMap;
                }
            }
        }
    }

    public long getSyncFull() {
        return extractAsLong(KEY_SYNC_FULL);
    }

    public long getSyncPartialOk() {
        return extractAsLong(KEY_SYNC_PARTIAL_OK);
    }

    public long getSyncPartialErr() {
        return extractAsLong(KEY_SYNC_PARTIAL_ERR);
    }

    public long getKeeperInstantaneousInputKbps() { return extractAsLong(KEY_INSTANTANEOUS_INPUT_KBPS);}

    public long getMasterReplOffset() {
        Long result = extractAsLong(KEY_MASTER_REPL_OFFSET);
        return result == null ? 0L : result;
    }

    public long getSlaveReplOffset() {
        Long result = extractAsLong(KEY_SLAVE_REPL_OFFSET);
        return result == null ? 0L : result;
    }
}
