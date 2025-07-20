package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.console.model.ConfigTblDao;
import com.ctrip.xpipe.redis.console.model.ConfigTblEntity;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.unidal.dal.jdbc.DalException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PlexusManualLoaderConfiguration.class)
public class DaoTest {

    @Autowired
    private PlexusContainer container;

    @Test
    public void test() throws ComponentLookupException, DalException, InitializationException {
        ConfigTblDao dao =   (ConfigTblDao) container.lookup(ConfigTblDao.class);
        dao.findByPK(1, ConfigTblEntity.READSET_FULL);
    }
}
