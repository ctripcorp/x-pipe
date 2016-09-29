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

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class KeepercontainerServiceTest extends AbstractRedisTest {
	@Mock
	private KeepercontainerTblDao mockedKeepercontainerTblDao;
	@InjectMocks
	private KeepercontainerService keepercontainerService;

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
		KeepercontainerTbl target_keepercontainer = new KeepercontainerTbl().setKeepercontainerId(1);

		assertEquals(keepercontainerService.findByDcName("NTGXH").get(0).getKeepercontainerId(),
				target_keepercontainer.getKeepercontainerId());
	}

	private void generateMetaMockData() throws DalException {
		when(mockedKeepercontainerTblDao.findByDcName("NTGXH", KeepercontainerTblEntity.READSET_FULL))
				.thenReturn(Arrays.asList(new KeepercontainerTbl().setKeepercontainerId(1)));
	}
}
