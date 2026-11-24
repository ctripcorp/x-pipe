package com.ctrip.xpipe.api.migration.auto.data;

public class MonitorForceFailoverResult {

    public static final int CODE_SUCCESS = 0;
    public static final int CODE_ERROR = -1;

    private int code;

    private String msg;

    private Object data;

    public MonitorForceFailoverResult(int code, String msg, Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public Object getData() {
        return data;
    }

    public boolean isSuccess() {
        return code == CODE_SUCCESS;
    }

    @Override
    public String toString() {
        return "MonitorForceFailoverResult{" + "code=" + code + ", msg='" + msg + '\'' + ", data=" + data + '}';
    }
}
