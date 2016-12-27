package com.ctrip.xpipe.service.migration;

import java.net.InetSocketAddress;
import java.util.Arrays;
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
		return restOperations.postForObject(
				CREDIS_SERVICE.MIGRATION_PUBLISH.getRealPath(MigrationPublishServiceConfig.INSTANCE.getCredisServiceAddress()),
				newMasters, MigrationPublishResult.class, clusterName, convertDcName(primaryDcName));
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
			InetSocketAddress newMaster) {
		logger.info("[doMigrationPublish]Cluster:{}, NewPrimaryDc:{} -> ConvertedDcName:{}, NewMaster:{}", clusterName, primaryDcName,convertDcName(primaryDcName), newMaster);
		return restOperations.postForObject(
				CREDIS_SERVICE.MIGRATION_PUBLISH.getRealPath(MigrationPublishServiceConfig.INSTANCE.getCredisServiceAddress()),
				Arrays.asList(newMaster), MigrationPublishResult.class, clusterName, convertDcName(primaryDcName));
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
