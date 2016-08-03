package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO;
import com.ctrip.xpipe.redis.console.web.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.web.model.DcTbl;
import com.ctrip.xpipe.redis.console.web.model.ShardTbl;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;

@RestController
public class ClusterController {

	@RequestMapping("/clusters/{clusterName}")
	public ClusterVO loadCluster(@PathVariable String clusterName){
		return mockData();

	}

	private ClusterVO mockData(){

		ClusterVO.Redis redis1 = new ClusterVO.Redis();
		redis1.setId("redis-0");
		redis1.setIp("127.1.1.1");
		redis1.setPort(8080);
		redis1.setRole(ClusterVO.RedisRole.MASTER);
		ClusterVO.Redis redis2 = new ClusterVO.Redis();
		redis2.setId("redis-1");
		redis2.setIp("127.1.1.2");
		redis2.setPort(8180);
		redis2.setRole(ClusterVO.RedisRole.KEPPER);


		ShardTbl shardTbl = new ShardTbl();
		shardTbl.setShardId("shard0");

		ClusterVO.Shard shard = new ClusterVO.Shard();
		shard.setBaseInfo(shardTbl);
		shard.setRedises(Arrays.asList(redis1, redis2));


		DcTbl dcTbl1 = new DcTbl();
		dcTbl1.setDcId("DC-JQ");
		ClusterVO.DC dc1 = new ClusterVO.DC();
		dc1.setBaseInfo(dcTbl1);
		dc1.setShards(Arrays.asList(shard, shard));

		DcTbl dcTbl2 = new DcTbl();
		dcTbl2.setDcId("DC-FQ");
		ClusterVO.DC dc2 = new ClusterVO.DC();
		dc2.setBaseInfo(dcTbl2);
		dc2.setShards(Arrays.asList(shard));

		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setActivedcId("DC-JQ");
		clusterTbl.setClusterId("cluster01");

		ClusterVO cluster = new ClusterVO();

		cluster.setBaseInfo(clusterTbl);
		cluster.setDcs(Arrays.asList(dc1, dc2));

		return cluster;

	}

}
