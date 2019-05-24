package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
//@RunWith(MockitoJUnitRunner.class)
public class DcServiceTest extends AbstractConsoleIntegrationTest {

	private static final String H2DB_DC_JQ = "jq";

	private static final String H2DB_DC_JQ_DESCRIPTION = "DC:jq";

	private static final long H2DB_DC_JQ_ZONE_ID = 1;

	@Autowired
	private DcService dcService;

	/**
	@Test
	public void testLoad() {
		DcTbl target_result = new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC")
				.setDcLastModifiedTime("1234567");

		assertEquals(dcService.find("NTGXH").getId(), target_result.getId());
		assertEquals(dcService.find("NTGXH").getClusterName(), target_result.getClusterName());

	}


	@Before
	public void initMockData() throws DalException {
		when(mockedDcTblDao.findDcByDcName("NTGXH", DcTblEntity.READSET_FULL)).thenReturn(
				new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC").setDcLastModifiedTime("1234567"));
	}*/

	@Test
	public void testFindByName(){
		DcTbl dcTbl = dcService.find(H2DB_DC_JQ);

		Assert.assertEquals(H2DB_DC_JQ_DESCRIPTION, dcTbl.getDcDescription());
		Assert.assertEquals(H2DB_DC_JQ_ZONE_ID, dcTbl.getZoneId());
	}

	@Test
	public void testCreateDc(){
		String dc_name = "test_dc";
		String description = "DC:test_dc";
		long zone_id = 1;
		dcService.insertWithPartField(zone_id, dc_name, description);

		DcTbl dcTbl = dcService.find(dc_name);

		Assert.assertEquals(dc_name, dcTbl.getDcName());
		Assert.assertEquals(description, dcTbl.getDcDescription());
		Assert.assertEquals(zone_id, dcTbl.getZoneId());
	}
}
