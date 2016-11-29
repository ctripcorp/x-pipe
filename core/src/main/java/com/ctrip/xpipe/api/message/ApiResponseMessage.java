package com.ctrip.xpipe.api.message;

/**
 * @author wenchao.meng
 *
 * Nov 29, 2016
 */
public class ApiResponseMessage {
	
	private int code;//0 for success, other code  
	
	private String message;

	public ApiResponseMessage(int code, String message){
		
		this.code = code;
		this.message = message;
	}

	public ApiResponseMessage(int code){
		this(code, null);
	}

	public int getCode() {
		return code;
	}
	public String getMessage() {
		return message;
	}

}
