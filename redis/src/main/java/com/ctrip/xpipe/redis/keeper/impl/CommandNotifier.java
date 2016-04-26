package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;

import com.ctrip.xpipe.redis.keeper.CommandsListener;

public interface CommandNotifier {

	void start(CommandStore store, long offset, CommandsListener listener) throws IOException;

	void close() throws IOException;

}
