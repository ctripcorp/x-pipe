package com.ctrip.xpipe.utils;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Trace;

/**
 * @author shyin
 *
 * Aug 11, 2016
 */
@Plugin(name="CatAppender4Log4j2", category="Core", elementType="appender", printObject=true)
public final class CatAppender4Log4j2 extends AbstractAppender{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param name
	 * @param filter
	 * @param layout
	 */
	protected CatAppender4Log4j2(String name, Filter filter, Layout<? extends Serializable> layout) {
		super(name, filter, layout);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see org.apache.logging.log4j.core.Appender#append(org.apache.logging.log4j.core.LogEvent)
	 */
	@Override
	public void append(LogEvent event) {
		boolean isTraceMode = Cat.getManager().isTraceMode();
		Level level = event.getLevel(); 

		if (level.isMoreSpecificThan(Level.ERROR)) {
			logError(event);
		} else if (isTraceMode) {
			logTrace(event);
		}
		
	}
	
	private String buildExceptionStack(Throwable exception) {
		if (exception != null) {
			StringWriter writer = new StringWriter(2048);

			exception.printStackTrace(new PrintWriter(writer));
			return writer.toString();
		} else {
			return "";
		}
	}

	private void logError(LogEvent event) {
		Throwable info = event.getThrown();

		if (info != null) {
			Throwable exception = info;
			Object message = event.getMessage();
			System.out.println(message);
			if (message != null) {
				
				Cat.logError(String.valueOf(message), exception);
			} else {
				Cat.logError(exception);
			}
		}
	}

	private void logTrace(LogEvent event) {
		String type = "Log4j";
		String name = event.getLevel().toString();
		Object message = event.getMessage();
		String data;

		if (message instanceof Throwable) {
			data = buildExceptionStack((Throwable) message);
		} else {
			data = event.getMessage().toString();
		}

		Throwable info = event.getThrown();

		if (info != null) {
			data = data + '\n' + buildExceptionStack(info);
		}
		Cat.logTrace(type, name, Trace.SUCCESS, data);
	}
	
	@PluginFactory
	public static CatAppender4Log4j2 createAppender(
			@PluginAttribute("name") String name,
			@PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginElement("Filter") final Filter filter) {
		return new CatAppender4Log4j2(name,filter,layout);
	}

}
