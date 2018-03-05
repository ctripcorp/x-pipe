package com.ctrip.xpipe.redis.console.job;

import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.job.retry.RetryCondition;
import com.ctrip.xpipe.redis.console.job.retry.RetryNTimesOnCondition;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2018
 */
public class AbstractRetryOnConditionTest {

    private RetryTemplate<String> retryTemplate;

    @Before
    public void beforeAbstractRetryOnConditionTest() {

    }

    @Test
    public void execute() throws Exception {
        RetryCondition<String> retryCondition = new RetryCondition.AbstractRetryCondition<String>() {
            @Override
            public boolean isSatisfied(String s) {
                try {
                    String[] strs = StringUtil.splitByLineRemoveEmpty(s);
                    return strs.length == 5;
                } catch (Exception e) {
                    return false;
                }
            }

            @Override
            public boolean isExceptionExpected(Throwable th) {
                return false;
            }
        };
        String result = new RetryNTimesOnCondition<>(retryCondition, 10).execute(new TestRetryOnConditionCommand());
        Assert.assertEquals("append\r\nappend\r\nappend\r\nappend\r\nappend\r\n", result);
    }



    class TestRetryOnConditionCommand extends AbstractCommand<String> {

        String str = "";

        @Override
        protected void doExecute() throws Exception {
            str += "append\r\n";
            future().setSuccess(str);
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return "TestRetryOnConditionCommand";
        }
    }

}