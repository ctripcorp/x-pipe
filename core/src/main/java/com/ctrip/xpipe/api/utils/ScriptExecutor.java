package com.ctrip.xpipe.api.utils;

import com.ctrip.xpipe.api.command.Command;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public interface ScriptExecutor<V> extends Command<V> {

    String getScript();

    V format(List<String> result);
}
