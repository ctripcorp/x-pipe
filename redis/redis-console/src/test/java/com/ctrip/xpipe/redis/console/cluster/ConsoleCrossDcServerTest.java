package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.xbill.DNS.TextParseException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 14, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleCrossDcServerTest extends AbstractConsoleTest{

    @Mock
    private ConsoleConfig consoleConfig;

    private ConsoleCrossDcServer crossDcClusterServer;

    private int checkIntervalMilli = 10;

    private List<String> cnames = new LinkedList<>();

    @Before
    public void beforeConsoleCrossDcServerTest(){


        crossDcClusterServer = new ConsoleCrossDcServer(){

            @Override
            protected List<String> lookUpCname(String domain) throws TextParseException {

                return cnames;
            }
        };

        crossDcClusterServer.setConsoleConfig(consoleConfig);
        crossDcClusterServer.setCheckIntervalMilli(checkIntervalMilli);

        cnames.add("cname1");
        HashMap<String, String> cnameToDc = new HashMap<>();
        cnameToDc.put("cname1", "jq");
        cnameToDc.put("cname2", "oy");
        DefaultFoundationService.setDataCenter("jq");

        when(consoleConfig.getConsoleDomain()).thenReturn("xpipe");
        when(consoleConfig.getConsoleCnameToDc()).thenReturn(cnameToDc);

    }


    @Test
    public void testNoCnameLeader() throws TimeoutException {

        cnames.clear();
        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

    }

    @Test
    public void testLeaderBecomeCrossDcLeader() throws TimeoutException {

        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

    }

    @Test
    public void testNotLeader() throws TimeoutException {

        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        crossDcClusterServer.notLeader();

        sleep(300);
        Assert.assertFalse(crossDcClusterServer.amILeader());

    }


    @Test
    public void testLeaderBecomeCrossDcLeaderThenFallback() throws TimeoutException {

        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        DefaultFoundationService.setDataCenter("oy");

        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader());
    }


    @Test
    public void test() throws TextParseException {

        try {
            List<String> cnames = crossDcClusterServer.lookUpCname("xpipe1.ctripcorp.com");
            logger.info("[test]{}", cnames);
        } catch (TextParseException e) {
            logger.error("[test]", e);
        }
    }
}
