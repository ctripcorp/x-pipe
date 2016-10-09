package com.ctrip.xpipe.redis.console.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class MetaserverServiceTest extends AbstractConsoleTest {
	@Mock
	private MetaserverTblDao mockedMetaserverTblDao;
	@InjectMocks
	private MetaserverService metaserverService;

	@Test
	public void testMetasService() {
		MetaserverTbl target_metaserver = new MetaserverTbl().setId(1).setMetaserverName("meta1");

		assertEquals(metaserverService.findByDcName("NTGXH").get(0).getMetaserverName(),
				target_metaserver.getMetaserverName());
	}

	@Before
	public void initMockData() throws Exception {
		when(mockedMetaserverTblDao.findByDcName("NTGXH", MetaserverTblEntity.READSET_FULL))
				.thenReturn(Arrays.asList(new MetaserverTbl().setId(1).setMetaserverName("meta1")));
	}
}
