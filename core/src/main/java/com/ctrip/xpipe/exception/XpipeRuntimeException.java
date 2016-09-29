package com.ctrip.xpipe.exception;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:58:53
 */
public class XpipeRuntimeException extends RuntimeException implements ErrorMessageAware{

	private static final long serialVersionUID = 1L;

	private ErrorMessage<?>  errorMessage;
	private boolean onlyLogMessage = false;

	public XpipeRuntimeException(String message){
		super(message);
	}
	
	public XpipeRuntimeException(String message, Throwable th){
		super(message, th);
	}

	public <T extends Enum<T>> XpipeRuntimeException(ErrorMessage<T> errorMessage, Throwable th){
		super(errorMessage.toString(), th);
		this.errorMessage = errorMessage;
	}

	@Override
	public ErrorMessage<?> getErrorMessage() {
		return errorMessage;
	}
	
	public void setErrorMessage(ErrorMessage<?> errorMessage) {
		this.errorMessage = errorMessage;
	}

	public boolean isOnlyLogMessage() {
		return onlyLogMessage;
	}

	public void setOnlyLogMessage(boolean onlyLogMessage) {
		this.onlyLogMessage = onlyLogMessage;
	}
}
