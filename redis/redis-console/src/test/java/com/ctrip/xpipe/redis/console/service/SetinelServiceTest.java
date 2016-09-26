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

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class SetinelServiceTest extends AbstractConsoleTest {
	@Mock
	private SetinelTblDao mockedSetinelTblDao;
	@InjectMocks
	private SetinelService setinelService;

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
		SetinelTbl target_setinel = new SetinelTbl().setSetinelId(1).setSetinelAddress("11111");

		assertEquals(setinelService.findByDcName("NTGXH").get(0).getSetinelAddress(),
				target_setinel.getSetinelAddress());
	}

	private void generateMetaMockData() throws DalException {
		when(mockedSetinelTblDao.findByDcName("NTGXH", SetinelTblEntity.READSET_FULL))
				.thenReturn(Arrays.asList(new SetinelTbl().setSetinelId(1).setSetinelAddress("11111")));
	}
}
