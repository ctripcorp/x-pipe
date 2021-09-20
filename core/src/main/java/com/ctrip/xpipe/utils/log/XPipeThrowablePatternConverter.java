package com.ctrip.xpipe.utils.log;


import com.ctrip.xpipe.exception.ExceptionUtils;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.core.pattern.ThrowablePatternConverter;
import org.apache.logging.log4j.util.Strings;

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
            String suffix = getSuffix(event);
            final String extStackTrace = proxy.getExtendedStackTraceAsString(options.getIgnorePackages(), options.getTextRenderer(), suffix);
            final int len = toAppendTo.length();
            if (len > 0 && !Character.isWhitespace(toAppendTo.charAt(len - 1))) {
                toAppendTo.append(' ');
            }
            if (!options.allLines() || !Strings.LINE_SEPARATOR.equals(options.getSeparator())) {
                final StringBuilder sb = new StringBuilder();
                final String[] array = extStackTrace.split(Strings.LINE_SEPARATOR);
                final int limit = options.minLines(array.length) - 1;
                for (int i = 0; i <= limit; ++i) {
                    sb.append(array[i]);
                    if (i < limit) {
                        sb.append(options.getSeparator());
                    }
                }
                toAppendTo.append(sb.toString());

            } else {
                toAppendTo.append(extStackTrace);
            }
        }
    }


}
