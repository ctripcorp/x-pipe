package com.ctrip.xpipe.exception;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:58:53
 */
public class XpipeException extends Exception implements ErrorMessageAware{

	private static final long serialVersionUID = 1L;
	private ErrorMessage<?>  errorMessage;
	
	public XpipeException(String message){
		super(message);
	}
	
	public XpipeException(String message, Throwable th){
		super(message, th);
	}
	
	public <T extends Enum<T>> XpipeException(ErrorMessage<T> errorMessage, Throwable th){
		super(errorMessage.toString(), th);
		this.errorMessage = errorMessage;
	}

	@Override
	public ErrorMessage<?> getErrorMessage() {
		return errorMessage;
	}
}
