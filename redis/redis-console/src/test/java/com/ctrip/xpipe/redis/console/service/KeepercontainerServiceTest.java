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
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class KeepercontainerServiceTest extends AbstractConsoleTest {
	@Mock
	private KeepercontainerTblDao mockedKeepercontainerTblDao;
	@InjectMocks
	private KeepercontainerService keepercontainerService;

	@Test
	public void testMetasService() {
		KeepercontainerTbl target_keepercontainer = new KeepercontainerTbl().setKeepercontainerId(1);

		assertEquals(keepercontainerService.findByDcName("NTGXH").get(0).getKeepercontainerId(),
				target_keepercontainer.getKeepercontainerId());
	}

	@Before
	public void initMockData() throws Exception {
		when(mockedKeepercontainerTblDao.findByDcName("NTGXH", KeepercontainerTblEntity.READSET_FULL))
				.thenReturn(Arrays.asList(new KeepercontainerTbl().setKeepercontainerId(1)));
	}
}
