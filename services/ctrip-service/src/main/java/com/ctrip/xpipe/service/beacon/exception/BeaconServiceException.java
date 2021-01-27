package com.ctrip.xpipe.service.beacon.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class BeaconServiceException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public BeaconServiceException(String reqUrl, int code, String msg) {
        super(String.format("beacon service resp for %s error %d:%s", reqUrl, code, msg));
    }

}
