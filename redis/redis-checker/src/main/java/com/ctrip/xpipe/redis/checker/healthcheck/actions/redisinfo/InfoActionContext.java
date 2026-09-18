package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.ParsableActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 10:08 PM
 */
public interface InfoActionContext extends ParsableActionContext<Map<String, String>, String, RedisHealthCheckInstance> {

    class Result extends ActionContextRetMessage<Map<String, String>> {}

    class ResultMap extends HashMap<HostPort, ActionContextRetMessage<Map<String, String>>> {}

    /**
     * Section 感知解析:section 为 null/空时全量返回(等同原行为);非空时只返回
     * 落在匹配 section(`# SectionName` 头标识,忽略大小写)内的 key:value。
     * 使用 split(":", 2) 避免值含冒号被截断。
     */
    static Map<String, String> parse(String info, String section) {
        Map<String, String> result = new HashMap<>();
        boolean sectionFilter = section != null && !section.isEmpty();
        String wanted = sectionFilter ? section.trim() : null;
        String currentSection = null;
        String[] lines = info.split("\r\n");
        for (String line : lines) {
            if (line.startsWith("# ")) {
                currentSection = line.substring(2).trim();
                continue;
            }
            String[] keyValues = line.split(":", 2);
            if (keyValues.length == 2) {
                if (!sectionFilter || (currentSection != null && currentSection.equalsIgnoreCase(wanted))) {
                    result.put(keyValues[0], keyValues[1]);
                }
            }
        }
        return result;
    }

    @Override
    default Map<String, String> parse(String info) {
        return parse(info, null);
    }

    /**
     * Section 感知版的 ActionContextRetMessage.from(ctx):success 时 payload 为
     * parse(inner().getResult(), section) 过滤后的 Map;fail 时 state=FAIL + cause message。
     */
    static Result toRetMessage(InfoActionContext ctx, String section) {
        Result ret = new Result();
        if (ctx.isSuccess()) {
            ret.setState(ActionContextRetMessage.SUCCESS_STATE);
            ret.setPayload(parse(ctx.inner().getResult(), section));
        } else {
            ret.setState(ActionContextRetMessage.FAIL_STATE);
            Throwable c = ctx.getCause();
            ret.setMessage(c == null ? "unknown" : c.getMessage());
        }
        return ret;
    }

    /**
     * 对多个 InfoActionContext 聚合为 ResultMap,逐个套 toRetMessage(ctx, section)。
     */
    static ResultMap toRetMessage(Map<HostPort, InfoActionContext> contexts, String section) {
        ResultMap result = new ResultMap();
        contexts.forEach((hp, ctx) -> result.put(hp, toRetMessage(ctx, section)));
        return result;
    }
}
