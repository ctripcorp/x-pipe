package com.ctrip.xpipe.api.proxy;

import com.ctrip.xpipe.api.command.Command;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Oct 23, 2018
 */
public interface ProxyCommand<T> extends Command<T> {

    ByteBuf getRequest();

}
