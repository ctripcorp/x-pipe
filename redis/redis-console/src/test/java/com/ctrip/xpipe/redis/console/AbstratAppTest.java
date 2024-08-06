package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Before;

import static com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean.KEY_METASERVERS;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 10, 2017
 */
public class AbstratAppTest extends AbstractConsoleDbTest {

    private String metaServers =
            "{ \"jq\" : \"http://127.0.0.1:9747\", " +
                    "\"oy\" : \"http://127.0.0.1:9748\" } ";


    @Before
    public void beforeAbstratAppTest(){
        System.setProperty(KEY_METASERVERS, metaServers);
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);

    }

}
