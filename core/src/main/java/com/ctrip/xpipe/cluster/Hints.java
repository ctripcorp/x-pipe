package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.utils.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @author ayq
 * <p>
 * 2022/11/9 15:38
 */
public enum Hints {

    APPLIER_IN_CLUSTER,
    MASTER_DC_IN_CLUSTER;

    public static Hints lookup(String name) {
        if (StringUtil.isEmpty(name)) throw new IllegalArgumentException("no Hints for name " + name);
        return valueOf(name.toUpperCase());
    }

    public static Set<Hints> parse(String str) {
        HashSet<Hints> result = new HashSet<>();
        if (StringUtils.isEmpty(str)) {
            return result;
        }
        for (String s : str.split("\\s*,\\s*")) {
            result.add(lookup(s));
        }
        return result;
    }

    public static String append(String str, Hints hints) {
        if (StringUtils.isEmpty(str)) {
            return hints.toString();
        }
        return str + "," + hints.toString();
    }
}
