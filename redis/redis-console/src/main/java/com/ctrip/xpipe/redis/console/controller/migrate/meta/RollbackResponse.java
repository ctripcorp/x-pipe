package com.ctrip.xpipe.redis.console.controller.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class RollbackResponse extends AbstractResponseMeta{

    private boolean success;

    private String msg;

    public RollbackResponse(){

    }

    public RollbackResponse(boolean success, String msg){
        this.success = success;
        this.msg = msg;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

}
