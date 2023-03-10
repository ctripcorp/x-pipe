package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.DcModel;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcClusterTypeStatisticsModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcListDcModel;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.*;

@Service
public class DcServiceImpl extends AbstractConsoleService<DcTblDao> implements DcService {

	@Autowired
	private DcMetaService dcMetaService;

	@Override
	public DcTbl find(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<DcTbl>() {
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findDcByDcName(dcName, DcTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public DcTbl find(final long dcId) {
		return queryHandler.handleQuery(new DalQuery<DcTbl>(){
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findByPK(dcId, DcTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public String getDcName(long dcId) {
		DcTbl dcTbl = find(dcId);
		if(dcTbl == null){
			throw new IllegalArgumentException("dc for dcid not found:" + dcId);
		}
		return dcTbl.getDcName();
	}

	@Override
	public List<DcTbl> findAllDcs() {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_BASIC);
			}
    	});
	}

	@Override
	public List<DcTbl> findAllDcNames() {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_NAME);
			}
    	});
	}

	@Override
	public List<DcTbl> findAllDcBasic() {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_BASIC);
			}
    	});
	}

	@Override
	public List<DcTbl> findClusterRelatedDc(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findClusterRelatedDc(clusterName, DcTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public DcTbl findByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<DcTbl>() {
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findDcByDcName(dcName, DcTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public Map<Long, String> dcNameMap() {

		List<DcTbl> allDcs = findAllDcs();
		Map<Long, String> result = new HashMap<>();

		allDcs.forEach(dcTbl -> result.put(dcTbl.getId(), dcTbl.getDcName()));
		return result;
	}

	@Override
	public Map<String, Long> dcNameIdMap() {
		List<DcTbl> allDcs = findAllDcs();
		Map<String, Long> result = new HashMap<>();

		allDcs.forEach(dcTbl -> result.put(dcTbl.getDcName(), dcTbl.getId()));
		return result;
	}


	@Override
	public Map<String, Long> dcNameZoneMap() {
		List<DcTbl> allDcs = findAllDcs();
		Map<String, Long> result = new HashMap<>();

		allDcs.forEach(dcTbl -> result.put(dcTbl.getDcName(), dcTbl.getZoneId()));
		return result;
	}

	@Override
	public List<DcListDcModel> findAllDcsRichInfo(){
		try {
			List<DcTbl> dcTbls = findAllDcs();
			if (dcTbls == null || dcTbls.size() == 0)
                return Collections.emptyList();

			List<DcListDcModel> result = new LinkedList<>();

			Map<String, DcMeta> dcMetaMap = dcMetaService.getAllDcMetas();

			dcTbls.forEach(dcTbl -> {
				DcMeta dcMeta = dcMetaMap.get(dcTbl.getDcName().toUpperCase());

				Map<String, List<ClusterMeta>> typeClusters = new HashMap<>();
				for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
					String type = clusterMeta.getType().toUpperCase();
					typeClusters.putIfAbsent(type, new ArrayList<>());
					typeClusters.get(type).add(clusterMeta);
				}

				List<DcClusterTypeStatisticsModel> dcClusterTypeClustersAnalyzers = new ArrayList<>();
				dcClusterTypeClustersAnalyzers.addAll(clusterTypesStatistics(dcMeta, typeClusters));
				dcClusterTypeClustersAnalyzers.add(totalStatistics(dcMeta.getId(), dcClusterTypeClustersAnalyzers));

				result.add(new DcListDcModel().setDcName(dcTbl.getDcName()).setDcDescription(dcTbl.getDcDescription()).setDcId(dcTbl.getId()).setClusterTypes(dcClusterTypeClustersAnalyzers));
			});

			return result;
		} catch (Exception e) {
			return Collections.emptyList();
		}
	}

	@Override
	public synchronized void insertWithPartField(long zoneId, String dcName, String description) {
		queryHandler.handleInsert(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.insertWithPartField(dao.createLocal().setZoneId(zoneId)
						.setDcName(dcName).setDcDescription(description)
						.setDataChangeLastTime(new Date()).setDcLastModifiedTime("")
				);
			}
		});
	}

	@Override
	public DcModel findDcModelByDcName(String dcName) {
		DcTbl dcTbl = find(dcName);
		return convertDcTblToDcModel(find(dcName));
	}

	@Override
	public DcModel findDcModelByDcId(long dcId) {
		return convertDcTblToDcModel(find(dcId));
	}

	@Override
	public void updateDcZone(DcModel dcModel) {
		DcTbl dcTbl = find(dcModel.getDc_name());
		if (dcTbl == null)
			throw new IllegalArgumentException(String.format("dc with name:%s not exist", dcModel.getDc_name()));

		dcTbl.setZoneId(dcModel.getZone_id());
		dcTbl.setDcDescription(dcModel.getDescription());

		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(dcTbl, DcTblEntity.UPDATESET_FULL);
			}
		});
	}

	private DcModel convertDcTblToDcModel(DcTbl dcTbl) {
		if (dcTbl == null) {
			return null;
		}

		DcModel dcModel = new DcModel();
		dcModel.setDc_name(dcTbl.getDcName());
		dcModel.setDescription(dcTbl.getDcDescription());
		dcModel.setZone_id(dcTbl.getZoneId());

		return dcModel;
	}

	private List<DcClusterTypeStatisticsModel> clusterTypesStatistics(DcMeta dcMeta,Map<String, List<ClusterMeta>> typeClusters) {
		List<DcClusterTypeStatisticsModel> allTypes = new ArrayList<>();
		typeClusters.forEach((k, v) -> {
			DcClusterTypeStatisticsModel analyzer = new DcClusterTypeStatisticsModel(dcMeta.getId(), k, v);
			if (ClusterType.lookup(k).supportKeeper()) {
				analyzer.setKeeperContainerCount(dcMeta.getKeeperContainers().size());
			}
			allTypes.add(analyzer);
			analyzer.analyse();
		});
		return allTypes;
	}

	private DcClusterTypeStatisticsModel totalStatistics(String dc, List<DcClusterTypeStatisticsModel> allTypes) {
		int allTypeClusterCount = 0, allTypeRedisCount = 0, allTypeKeeperCount = 0, allTypeClusterInActiveDcCount = 0, allTypeKeeperContainerCount = 0;
		for (DcClusterTypeStatisticsModel model : allTypes) {
			allTypeClusterCount += model.getClusterCount();
			allTypeRedisCount += model.getRedisCount();
			allTypeKeeperCount += model.getKeeperCount();
			allTypeClusterInActiveDcCount += model.getClusterInActiveDcCount();
			allTypeKeeperContainerCount += model.getKeeperContainerCount();
		}
		return new DcClusterTypeStatisticsModel().
				setClusterType("").
				setDcName(dc).
				setClusterCount(allTypeClusterCount).
				setRedisCount(allTypeRedisCount).
				setKeeperCount(allTypeKeeperCount).
				setClusterInActiveDcCount(allTypeClusterInActiveDcCount).
				setKeeperContainerCount(allTypeKeeperContainerCount);

	}

}
