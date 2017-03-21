package com.ctrip.xpipe.service.migration;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.web.client.RestOperations;

import com.ctrip.xpipe.migration.AbstractMigrationPublishService;
import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public class CredisMigrationPublishService extends AbstractMigrationPublishService {

	RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(10, 100);
	
	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) {
		
		logger.info("[doMigrationPublish]Cluster:{}, NewPrimaryDc:{} -> ConvertedDcName:{} , NewMasters:{}", clusterName, primaryDcName,convertDcName(primaryDcName), newMasters);
		
		String credisAddress = CREDIS_SERVICE.MIGRATION_PUBLISH.getRealPath(MigrationPublishServiceConfig.INSTANCE.getCredisServiceAddress());
		String startTime = sdf.format(new Date());
		MigrationPublishResult res = restOperations.postForObject(credisAddress,
				newMasters, MigrationPublishResult.class, clusterName, convertDcName(primaryDcName));
		String endTime = sdf.format(new Date());
		res.setPublishAddress(credisAddress);
		res.setClusterName(clusterName);
		res.setPrimaryDcName(primaryDcName);
		res.setNewMasters(newMasters);
		res.setStartTime(startTime);
		res.setEndTime(endTime);
		return res;
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
			InetSocketAddress newMaster) {
		return doMigrationPublish(clusterName, primaryDcName, Arrays.asList(newMaster));
	}
	
	String convertDcName(String dc) {
		Map<String, String> idsMappingRules = MigrationPublishServiceConfig.INSTANCE.getCredisIdcMappingRules();
		if(idsMappingRules.containsKey(dc.toUpperCase())) {
			return idsMappingRules.get(dc.toUpperCase());
		} else {
			return dc;
		}
	}
	
}
