package com.ctrip.xpipe.utils.log;

import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Trace;
import com.dianping.cat.message.spi.MessageManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

/**
 * @author shyin
 *
 *         Aug 11, 2016
 */
@Plugin(name = "CatAppender4Log4j2", category = "Core", elementType = "appender", printObject = true)
public final class CatAppender4Log4j2 extends AbstractAppender {

	/**
	 * @param name
	 * @param filter
	 * @param layout
	 */
	protected CatAppender4Log4j2(String name, Filter filter, Layout<? extends Serializable> layout) {
		super(name, filter, layout);
	}

	@Override
	public void append(LogEvent event) {
		
		MessageManager messageManager = Cat.getManager();
		
		boolean isTraceMode = false;
		if(messageManager != null){
			isTraceMode = messageManager.isTraceMode();
		}
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
			String extra = ExceptionUtils.extractExtraMessage(info);

			String logMessage = StringUtil.join(",", message, extra);
			
			if (!StringUtil.isEmpty(logMessage)) {
				Cat.logError(logMessage, exception);
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
			String 	  extra = ExceptionUtils.extractExtraMessage(info);
			if(extra != null){
				data += "\n" + extra;
			}
			data += '\n' + buildExceptionStack(info);
		}
		Cat.logTrace(type, name, Trace.SUCCESS, data);
	}

	@PluginFactory
	public static CatAppender4Log4j2 createAppender(@PluginAttribute("name") String name,
			@PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginElement("Filter") final Filter filter) {
		return new CatAppender4Log4j2(name, filter, layout);
	}

}
