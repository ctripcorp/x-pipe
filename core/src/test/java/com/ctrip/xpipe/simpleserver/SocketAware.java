package com.ctrip.xpipe.simpleserver;

import java.net.Socket;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午6:35:40
 */
public interface SocketAware {

	Socket getSocket();
}
