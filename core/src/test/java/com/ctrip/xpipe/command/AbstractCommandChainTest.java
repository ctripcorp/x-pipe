package com.ctrip.xpipe.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public class AbstractCommandChainTest extends AbstractTest{

	protected Command<?>[] createSuccessCommands(int count, String message) {
		
		return createSuccessCommands(count, message, 100);
	}

	
	protected Command<?>[] createSuccessCommands(int count, String message, int sleepIntervalMilli) {
		
		Command<?>[] result = new Command<?>[count]; 
		for(int i = 0; i < count ;i++){
			result[i] = new TestCommand(message, sleepIntervalMilli);
		}
		return result;
	}

	protected Command<?>[] createCommands(int count, String successMessage, int failIndex, Exception exception) {
		return createCommands(count, successMessage, failIndex, exception, 100);
	}

	protected Command<?>[] createCommands(int count, String successMessage, int failIndex, Exception exception, int sleepIntervalMilli) {
		
		Command<?>[] result = new Command<?>[count]; 
		for(int i = 0; i < count ;i++){
			if(i == failIndex){
				result[i] = new TestCommand(exception, sleepIntervalMilli);
			}else{
				result[i] = new TestCommand(successMessage, sleepIntervalMilli);
			}
		}
		return result;
	}




}
