package com.ctrip.xpipe.api.utils;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public interface ScriptExecutor<V> extends Command {

    String getScript();
}
