package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;
import com.ctrip.xpipe.redis.console.service.impl.KeeperContainerServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerCheckerServiceTest extends AbstractConsoleTest {
	@Mock
	private KeepercontainerTblDao mockedKeepercontainerTblDao;
	@InjectMocks
	private KeeperContainerServiceImpl keeperContainerService;

	@Test
	public void testMetasService() {
		KeepercontainerTbl target_keepercontainer = new KeepercontainerTbl().setKeepercontainerId(1);

		assertEquals(keeperContainerService.findAllByDcName("NTGXH").get(0).getKeepercontainerId(),
				target_keepercontainer.getKeepercontainerId());
	}

	@Before
	public void initMockData() throws Exception {
		when(mockedKeepercontainerTblDao.findByDcName("NTGXH", KeepercontainerTblEntity.READSET_FULL))
				.thenReturn(Arrays.asList(new KeepercontainerTbl().setKeepercontainerId(1)));
	}
}
