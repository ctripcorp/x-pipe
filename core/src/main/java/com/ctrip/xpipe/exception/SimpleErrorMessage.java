package com.ctrip.xpipe.exception;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 20, 2017
 */
public class SimpleErrorMessage extends ErrorMessage<SIMPLE_RETURN_CODE>{

    public SimpleErrorMessage(){}


    public SimpleErrorMessage(SIMPLE_RETURN_CODE errorType, String errorMessage) {
        super(errorType, errorMessage);
    }

    public static SimpleErrorMessage success(){
        return new SimpleErrorMessage(SIMPLE_RETURN_CODE.SUCCESS, "success!");
    }

    public static SimpleErrorMessage fail(String reason){
        return new SimpleErrorMessage(SIMPLE_RETURN_CODE.FAIL, reason);
    }

}
