package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcListDcModel;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
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

			dcTbls.forEach(dcTbl -> {
				DcMeta dcMeta = dcMetaService.getDcMeta(dcTbl.getDcName());

				DcListDcModel dcModel = new DcListDcModel();
				int countRedisNums = 0, countKeeperNums = 0, countClusterInActiveDc = 0;
				dcModel.setDcId(dcTbl.getId());
				dcModel.setClusterCount(dcMeta.getClusters().values().size());
				dcModel.setDcName(dcMeta.getId());
				dcModel.setKeeperContainerCount(dcMeta.getKeeperContainers().size());
				for (ClusterMeta clusterMeta: dcMeta.getClusters().values()){
					if (dcTbl.getDcName().equals(clusterMeta.getActiveDc()))
						countClusterInActiveDc++;

					for (ShardMeta shardMeta: clusterMeta.getShards().values()){
						if (-countRedisNums <= Integer.MIN_VALUE || -countRedisNums - shardMeta.getRedises().size() <= Integer.MIN_VALUE){
							throw new XpipeRuntimeException(String.format("redis numbers overflow: %d", countRedisNums));
						}else if (-countKeeperNums <= Integer.MIN_VALUE || -countKeeperNums - shardMeta.getKeepers().size() <= Integer.MIN_VALUE){
							throw new XpipeRuntimeException(String.format("keeper numbers overflow: %d", countKeeperNums));
						}else{
							countRedisNums  += shardMeta.getRedises().size();
							countKeeperNums += shardMeta.getKeepers().size();
						}

					}
				}
				dcModel.setRedisCount(countRedisNums);
				dcModel.setKeeperCount(countKeeperNums);
				dcModel.setClusterInActiveDcCount(countClusterInActiveDc);
				result.add(dcModel);
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

}
