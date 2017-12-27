package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.utils.MapUtils;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
public class SentinelServiceImpl extends AbstractConsoleService<SetinelTblDao> implements SentinelService {

	private DcClusterShardTblDao dcClusterShardTblDao;

	@Autowired
    private ClusterService clusterService;

	private Random random;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Dao construct failed.", e);
		}
	}
	
	@Override
	public List<SetinelTbl> findAllByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, SetinelTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public Map<Long, List<SetinelTbl>> allSentinelsByDc() {

		Map<Long, List<SetinelTbl>> result = new HashMap<>();

		List<SetinelTbl> setinelTbls = queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return dao.findAll(SetinelTblEntity.READSET_FULL);
			}
		});

		setinelTbls.forEach( setinelTbl -> {

			List<SetinelTbl> setinels = MapUtils.getOrCreate(result, setinelTbl.getDcId(), new ObjectFactory<List<SetinelTbl>>() {
				@Override
				public List<SetinelTbl> create() {
					return new LinkedList<>();
				}
			});
			setinels.add(setinelTbl);
		});

		return result;
	}

	@Override
	public Map<Long, SetinelTbl> eachRandomSentinelByDc() {

		Map<Long, List<SetinelTbl>> allSentinelsByDc = allSentinelsByDc();
		
		Map<Long, SetinelTbl>  result = randomChose(allSentinelsByDc);

		return result;
	}

	private Map<Long,SetinelTbl> randomChose(Map<Long, List<SetinelTbl>> allSentinelsByDc) {

		Map<Long, SetinelTbl>  result = new HashMap<>();

		allSentinelsByDc.forEach((dcId, setinels) -> {
			result.put(dcId, random(setinels));
		});

		return result;
	}

	protected SetinelTbl random(List<SetinelTbl> setinels) {

		Random random = new Random();

		int index = random.nextInt(setinels.size());

		return setinels.get(index);
	}

	@Override
	public SetinelTbl find(final long id) {
		return queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
			@Override
			public SetinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SetinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public Map<Long, SetinelTbl> findByShard(long shardId) {
		List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findAllByShardId(shardId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
		
		Map<Long, SetinelTbl> res = new HashMap<>(dcClusterShards.size());
		for(DcClusterShardTbl dcClusterShard : dcClusterShards) {
			SetinelTbl sentinel = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
				@Override
				public SetinelTbl doQuery() throws DalException {
					return dao.findByPK(dcClusterShard.getSetinelId(), SetinelTblEntity.READSET_FULL);
				}
			});
			if(null != sentinel) {
				res.put(sentinel.getDcId(), sentinel);
			}
		}
		return res;
	}

	@Override
	public SetinelTbl insert(SetinelTbl setinelTbl) {

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.insert(setinelTbl);
			}
		});

		return setinelTbl;
	}

	@Override
	public List<String> reBalanceSentinels(String dcName, int numOfClusters) {
		List<String> clusters = randomlyChosenClusters(clusterService.findAllClusterNames(), numOfClusters);
		logger.info("[reBalanceSentinels] pick up clusters: {}", clusters);
		doReBalance(dcName, clusters);
		return clusters;
	}

	private List<String> randomlyChosenClusters(List<String> clusters, int num) {
	    if(num < 1 || clusters == null || clusters.isEmpty()) return clusters;
	    if(random == null) {
	        random = new Random();
        }
        int bound = clusters.size(), index = random.nextInt(bound);
	    Set<String> result = new HashSet<>();
	    for(int count = 0; count < num; count++) {
	        while (!result.add(clusters.get(index))) {
                index = random.nextInt(bound);
            }
        }
        return new LinkedList<>(result);
    }

    private void doReBalance(String dcName, List<String> cluster) {

    }
}
