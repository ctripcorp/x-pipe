package com.ctrip.xpipe.redis.console.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author shyin
 *
 * Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class MetaserverServiceTest extends AbstractRedisTest{
	@Mock
	private MetaserverTblDao mockedMetaserverTblDao;
	@InjectMocks
	private MetaserverService metaserverService;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		
		try {
			generateMetaMockData();
		} catch (Exception e) {
			logger.error("Generate Dc mock data failed.", e);
		}
	}
	
	@Test
	public void testMetasService() {
		MetaserverTbl target_metaserver = new MetaserverTbl().setId(1).setMetaserverName("meta1");
		
		assertEquals(metaserverService.findByDcName("NTGXH").get(0).getMetaserverName(), target_metaserver.getMetaserverName());
	}
	
	private void generateMetaMockData() throws DalException {
		when(mockedMetaserverTblDao.findByDcName("NTGXH", MetaserverTblEntity.READSET_FULL)).thenReturn(
				Arrays.asList(new MetaserverTbl().setId(1).setMetaserverName("meta1")));
	}
}
