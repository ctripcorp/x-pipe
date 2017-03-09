package com.ctrip.xpipe.simpleserver;

import java.net.Socket;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午6:37:11
 */
public interface IoActionFactory {

	IoAction createIoAction(Socket socket);
}
