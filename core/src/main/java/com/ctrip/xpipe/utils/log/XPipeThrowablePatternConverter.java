package com.ctrip.xpipe.utils.log;


import com.ctrip.xpipe.exception.ExceptionUtils;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.core.pattern.ThrowablePatternConverter;

/**
 * @author wenchao.meng
 *
 * Nov 1, 2016
 */
@Plugin(name = "XpipeThrowablePatternConverter", category = PatternConverter.CATEGORY)
@ConverterKeys({ "xpEx", "xpThrowable", "xpException" })
public final class XPipeThrowablePatternConverter extends ThrowablePatternConverter{
	
    /**
     * Private constructor.
     *
     * @param options options, may be null.
     */
    private XPipeThrowablePatternConverter(final Configuration config, final String[] options) {
        super("ExtendedThrowable", "throwable", options, config);
    }

    /**
     * Gets an instance of the class.
     *
     * @param options pattern options, may be null.  If first element is "short",
     *                only the first line of the throwable will be formatted.
     * @return instance of class.
     */
    public static XPipeThrowablePatternConverter newInstance(final Configuration config, final String[] options) {
        return new XPipeThrowablePatternConverter(config, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void format(final LogEvent event, final StringBuilder toAppendTo) {
        final ThrowableProxy proxy = event.getThrownProxy();
        final Throwable throwable = event.getThrown();

        //xpipe code
        if(throwable != null){
	    	if(ExceptionUtils.isSocketIoException(event.getThrown()) || ExceptionUtils.xpipeExceptionLogMessage(throwable)){
	    		toAppendTo.append("," + throwable.getClass() + ":" + throwable.getMessage());
	    		return;
	    	}

	    	String extra = ExceptionUtils.extractExtraMessage(throwable);
	    	if(extra != null){
				toAppendTo.append(String.format("\n[%s]", extra));
			}
        }

        if ((throwable != null || proxy != null) && options.anyLines()) {
            if (proxy == null) {
                super.format(event, toAppendTo);
                return;
            }
            final int len = toAppendTo.length();
            if (len > 0 && !Character.isWhitespace(toAppendTo.charAt(len - 1))) {
                toAppendTo.append(' ');
            }

            proxy.formatExtendedStackTraceTo(toAppendTo, options.getIgnorePackages(),
                    options.getTextRenderer(), getSuffix(event), options.getSeparator());
        }
    }
}
