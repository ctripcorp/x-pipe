package com.ctrip.xpipe.exception;

import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Aug 11, 2016
 */
public class ErrorMessage<T extends Enum<T>> {
	
	private T errorType;
	private String errorMessage;
	
	//for deserialize
	public ErrorMessage(){}


	public ErrorMessage(T errorType, String errorMessage) {
		this.errorType = errorType;
		this.errorMessage = errorMessage;
	}
	
	public T getErrorType() {
		return errorType;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	@Override
	public boolean equals(Object obj) {
		
		if(!(obj instanceof ErrorMessage<?>)){
			return false;
		}
		
		ErrorMessage<?> other = (ErrorMessage<?>) obj;
		if(!(ObjectUtils.equals(errorType, other.errorType))){
			return false;
		}
		if(!(ObjectUtils.equals(errorMessage, other.errorMessage))){
			return false;
		}
		return true;
	}
	
	@Override
	public int hashCode() {
		return ObjectUtils.hashCode(errorMessage, errorType);
	}
	
	@Override
	public String toString() {
		return String.format("code:%s, message:%s", errorType, errorMessage);
	}
}
