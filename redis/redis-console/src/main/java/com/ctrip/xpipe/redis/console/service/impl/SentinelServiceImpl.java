package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.function.Function;

@Service
public class SentinelServiceImpl extends AbstractConsoleService<SetinelTblDao> implements SentinelService {

	private DcClusterShardTblDao dcClusterShardTblDao;
	
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
	public Map<String, SentinelUsageModel> getAllSentinelsUsage() {
		List<SetinelTbl> sentinels = queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return dao.findSentinelUsage(SetinelTblEntity.READSET_SENTINEL_USAGE);
			}
		});
		Map<String, SentinelUsageModel> result = Maps.newHashMapWithExpectedSize(sentinels.size());
		for(SetinelTbl sentinelTbl : sentinels) {
			if(StringUtil.isEmpty(sentinelTbl.getSetinelAddress()))
				continue;
			String dcName = sentinelTbl.getDcInfo().getDcName();
			result.putIfAbsent(dcName, new SentinelUsageModel(dcName));
			SentinelUsageModel usage = result.get(dcName);
			usage.addSentinelUsage(sentinelTbl.getSetinelAddress(), sentinelTbl.getCount());
		}
		return result;
	}

	@Override
	public SentinelModel updateSentinelTblAddr(SentinelModel sentinel) {
		SetinelTbl target = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
			@Override
			public SetinelTbl doQuery() throws DalException {
				return dao.findByPK(sentinel.getId(), SetinelTblEntity.READSET_FULL);
			}
		});
		if(target == null) {
			throw new IllegalArgumentException("no sentinel found due to id: " + sentinel.getId());
		}
		target.setSetinelAddress(StringUtil.join(",", new Function<HostPort, String>() {
			@Override
			public String apply(HostPort hostPort) {
				return hostPort.toString();
			}
		}, sentinel.getSentinels()));
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateSentinelAddr(target, SetinelTblEntity.UPDATESET_ADDRESS);
			}
		});

		return queryHandler.handleQuery(new DalQuery<SentinelModel>() {
			@Override
			public SentinelModel doQuery() throws DalException {
				return new SentinelModel(dao.findByPK(target.getSetinelId(), SetinelTblEntity.READSET_FULL));
			}
		});
	}

	@Override
	public void delete(long id) {
		SetinelTbl setinelTbl = dao.createLocal();
		setinelTbl.setSetinelId(id);
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.deleteSentinel(setinelTbl, SetinelTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void reheal(long id) {
		SetinelTbl setinelTbl = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
			@Override
			public SetinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SetinelTblEntity.READSET_FULL);
			}
		});
		setinelTbl.setDeleted(false);
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(setinelTbl, SetinelTblEntity.UPDATESET_FULL);
			}
		});
	}
}
