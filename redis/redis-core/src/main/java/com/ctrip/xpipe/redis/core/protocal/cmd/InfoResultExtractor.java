package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private static final String KEY_SWAP_USED_DB_SIZE = "swap_used_db_size";
    private static final String KEY_USED_MEMORY ="used_memory";
    private static final String KEY_MAX_MEMORY ="maxmemory";
    private static final String KEY_KEEPER_ACTIVE = "state";
    private static final String KEY_SWAP_VERSION = "swap_version";
    private static final String KEY_KEEPER_MASTER_HOST = "master_host";
    private static final String KEY_KEEPER_MASTER_PORT = "master_port";
    private static final String KEY_KEEPER_REPL_BACKLOG_SIZE = "repl_backlog_size";
    private static final String KEY_SLAVE = "slave";

    protected static Logger logger = LoggerFactory.getLogger(InfoResultExtractor.class);

    private String result;
    private Map<String, String> keyValues;

    public InfoResultExtractor(String result) {
        this.result = result;
    }

    public boolean contain(String key) {
        genKeyValues();
        return keyValues.containsKey(key);
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

    public Float extractAsFloat(String key) {
        return extract(key, (value) -> value == null ? null : Float.parseFloat(value));
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

    public boolean isROR() {
        return contain(KEY_SWAP_VERSION);
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

    public Float getKeeperInstantaneousInputKbps() { return extractAsFloat(KEY_INSTANTANEOUS_INPUT_KBPS);}

    public long getUsedMemory() { return extractAsLong(KEY_USED_MEMORY);}

    public long getMaxMemory() { return extractAsLong(KEY_MAX_MEMORY);}

    public Long getSwapUsedDbSize() { return extractAsLong(KEY_SWAP_USED_DB_SIZE);}

    public boolean isKeeperActive() { return "ACTIVE".equals(extract(KEY_KEEPER_ACTIVE)); }

    public String getKeySwapVersion() {
        return extract(KEY_SWAP_VERSION);
    }

    public long getMasterReplOffset() {
        Long result = extractAsLong(KEY_MASTER_REPL_OFFSET);
        return result == null ? 0L : result;
    }

    public long getSlaveReplOffset() {
        Long result = extractAsLong(KEY_SLAVE_REPL_OFFSET);
        return result == null ? 0L : result;
    }

    public String getKeeperState() {
        return extract(KEY_KEEPER_ACTIVE);
    }

    public String getKeyKeeperMasterHost() {
        return extract(KEY_KEEPER_MASTER_HOST);
    }

    public int getKeyKeeperMasterPort() {
        Integer result = extractAsInteger(KEY_KEEPER_MASTER_PORT);
        return result == null ? 0 : result;
    }

    public long getKeyKeeperReplBacklogSize() {
        Long result = extractAsLong(KEY_KEEPER_REPL_BACKLOG_SIZE);
        return result == null ? 0L : result;
    }

    public List<Map<String, String>> getKeyKeeperSlaves() {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            if (entry.getKey().length() >= 5 && KEY_SLAVE.equals(entry.getKey().substring(0, 5))) {
                Map<String, String> map = new HashMap<>();
                for (String element : entry.getValue().split(",")) {
                    String[] split = element.split("=");
                    if (split.length != 2) continue;
                    map.put(split[0], split[1]);
                }
                if (!map.isEmpty()) {
                    result.add(map);
                }
            }
        }
        return result;
    }
}
