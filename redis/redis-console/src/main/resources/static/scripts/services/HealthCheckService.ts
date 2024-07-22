angular
	.module('services')
	.service('HealthCheckService', HealthCheckService);

HealthCheckService.$inject = ['$resource', '$q'];

function HealthCheckService($resource, $q) {
	const IS_REDIS_HEALTH = "is_redis_health";
	const GET_REPL_DELAY = "get_repl_delay";
	const GET_SHARD_DELAY = "get_shard_delay";
	const GET_HICKWALL_ADDR = "get_hickwall_addr";
	const GET_HETERO_HICKWALL_ADDR = "get_hetero_hickwall_addr";
	const GET_CROSS_MASTER_DELAY = "get_cross_master_delay";
	const GET_CROSS_MASTER_HICKWALL_ADDR = "get_cross_master_hickwall_addr";
	const GET_PEER_OUTCOMING = "get_peer_outcoming";
	const GET_PEER_INCOMING = "get_peer_incoming";
	const GET_PEER_FULL_SYNC = "get_peer_full_sync";
	const GET_PEER_PARTIAL_SYNC = "get_peer_partial_sync";
	const GET_SHARD_REDIS_ROLE = "get_shard_redis_role";
	const GET_SHARD_KEEPER_STATE = "get_shard_keeper_state";
	const GET_SHARD_CHECKER_HEALTH_CHECK = "get_shard_checker_health_check";
	const GET_SHARD_ALL_META = "get_shard_all_meta";

	var apis = (function initResourceParam() {
		var apis = {};
		apis[IS_REDIS_HEALTH] = {
			method: 'GET',
			url: '/console/redis/health/:redisIp/:redisPort'
		};
		apis[GET_REPL_DELAY] = {
			method: 'GET',
			url: '/console/redis/delay/:clusterType/:redisIp/:redisPort'
		};
		apis[GET_SHARD_DELAY] = {
			method: 'GET',
			url: '/console/shard/delay/:clusterId/:shardId/:shardDbId'
		};
		apis[GET_HICKWALL_ADDR] = {
			method: 'GET',
			url: '/console/redis/health/hickwall/:cluster/:shard/:redisIp/:redisPort/:delayType'
		};
		apis[GET_HETERO_HICKWALL_ADDR] = {
			method: 'GET',
			url: '/console/hetero/health/hickwall/:cluster/:srcShardId/:delayType'
		};
		apis[GET_CROSS_MASTER_DELAY] = {
			method: 'GET',
			url: '/console/cross-master/delay/:clusterType/:dc/:cluster/:shard'
		};
		apis[GET_CROSS_MASTER_HICKWALL_ADDR] = {
			method: 'GET',
			url: '/console/cross-master/health/hickwall/:cluster/:shard/:sourceDc/:destDc'
		};
		apis[GET_PEER_OUTCOMING] = {
			method: 'GET',
			url: '/console/redis/outcoming/traffic/to/peer/hickwall/:redisIp/:redisPort'
		};
		apis[GET_PEER_INCOMING] = {
			method: 'GET',
			url: '/console/redis/incoming/traffic/from/peer/hickwall/:redisIp/:redisPort'
		};
		apis[GET_PEER_FULL_SYNC] = {
			method: 'GET',
			url: '/console/redis/peer/sync/full/hickwall/:redisIp/:redisPort'
		};
		apis[GET_PEER_PARTIAL_SYNC] = {
			method: 'GET',
			url: '/console/redis/peer/sync/partial/hickwall/:redisIp/:redisPort'
		};
		apis[GET_SHARD_REDIS_ROLE] = {
			method: 'GET',
			url: '/api/redis/role/:dcId/:clusterId/:shardId',
			isArray : true
		};
		apis[GET_SHARD_KEEPER_STATE] = {
			method: 'GET',
			url: '/api/keeper/states/:dcId/:clusterId/:shardId',
			isArray : true
		};
		apis[GET_SHARD_CHECKER_HEALTH_CHECK] = {
			method: 'GET',
			url: '/api/checker/groups/:dcId/:clusterId/:shardId',
			isArray : true
		};
		apis[GET_SHARD_ALL_META] = {
			method: 'GET',
			url: '/api/meta/all/:dcId/:clusterId/:shardId'
		};
		return apis;
	})();
	var resource = $resource('', {}, apis);

	function request(q, method, params) {
		resource[method](params, function(result) {
			q.resolve(result)
		}, function(result) {
			q.reject(result)
		});
		return q.promise;
	}

	function isRedisHealth(ip,port) {
		return request($q.defer(), IS_REDIS_HEALTH, {
			redisIp : ip,
			redisPort: port
		});
	}
	
	function getReplDelay(ip, port, clusterType) {
		return request($q.defer(), GET_REPL_DELAY, {
			clusterType,
			redisIp : ip,
			redisPort : port
		});
	}

	function getShardDelay(clusterId, shardId, shardDbId) {
		return request($q.defer(), GET_SHARD_DELAY, {
			clusterId: clusterId,
			shardId: shardId,
			shardDbId: shardDbId
		});
	}
	
	function getHickwallAddr(cluster, shard, redisIp, redisPort, delayType) {
		return request($q.defer(), GET_HICKWALL_ADDR, {
			cluster : cluster,
			shard : shard,
			redisIp : redisIp,
			redisPort : redisPort,
			delayType: delayType
		});
	}

	function getHeteroHickwallAddr(cluster, srcShardId, delayType) {
		return request($q.defer(), GET_HETERO_HICKWALL_ADDR, {
			cluster : cluster,
			srcShardId : srcShardId,
			delayType: delayType
		});
	}

	function getCrossMasterDelay(dc, cluster, shard, clusterType) {
		return request($q.defer(), GET_CROSS_MASTER_DELAY, {
			dc, cluster, shard, clusterType
		});
	}

	function getCrossMasterHickwallAddr(cluster, shard, sourceDc, destDc) {
		return request($q.defer(), GET_CROSS_MASTER_HICKWALL_ADDR, {
			cluster, shard, sourceDc, destDc
		});
	}
	

	function getOutComingTrafficToPeerHickwallAddr(redisIp, redisPort) {
		return request($q.defer(), GET_PEER_OUTCOMING, {
			redisIp,
			redisPort
		});
	}

	function getInComingTrafficFromPeerHickwallAddr(redisIp, redisPort) {
		return request($q.defer(), GET_PEER_INCOMING, {
			redisIp,
			redisPort
		});
	}

	function getPeerSyncFullHickwallAddr(redisIp, redisPort) {
		return request($q.defer(), GET_PEER_FULL_SYNC, {
			redisIp,
			redisPort
		});
	}

	function getPeerSyncPartialHickwallAddr(redisIp, redisPort) {
		return request($q.defer(), GET_PEER_PARTIAL_SYNC, {
			redisIp,
			redisPort
		});
	}

	function getShardRedisRole(dcId, clusterId, shardId) {
		return request($q.defer(), GET_SHARD_REDIS_ROLE, {
			dcId: dcId,
			clusterId: clusterId,
			shardId: shardId
		});
	}

	function getShardKeeperState(dcId, clusterId, shardId) {
		return request($q.defer(), GET_SHARD_KEEPER_STATE, {
			dcId: dcId,
			clusterId: clusterId,
			shardId: shardId
		});
	}

	function getShardCheckerHealthCheck(dcId, clusterId, shardId) {
		return request($q.defer(), GET_SHARD_CHECKER_HEALTH_CHECK, {
			dcId: dcId,
			clusterId: clusterId,
			shardId: shardId
		});
	}

	function getShardAllMeta(dcId, clusterId, shardId) {
		return request($q.defer(), GET_SHARD_ALL_META, {
			dcId: dcId,
			clusterId: clusterId,
			shardId: shardId
		});
	}

	return {
		isRedisHealth : isRedisHealth,
		getReplDelay : getReplDelay,
		getShardDelay: getShardDelay,
		getHickwallAddr : getHickwallAddr,
		getHeteroHickwallAddr: getHeteroHickwallAddr,
		getCrossMasterDelay : getCrossMasterDelay,
		getCrossMasterHickwallAddr: getCrossMasterHickwallAddr,
		getOutComingTrafficToPeerHickwallAddr: getOutComingTrafficToPeerHickwallAddr,
		getInComingTrafficFromPeerHickwallAddr: getInComingTrafficFromPeerHickwallAddr,
		getPeerSyncFullHickwallAddr: getPeerSyncFullHickwallAddr,
		getPeerSyncPartialHickwallAddr: getPeerSyncPartialHickwallAddr,
		getShardRedisRole: getShardRedisRole,
		getShardKeeperState: getShardKeeperState,
		getShardCheckerHealthCheck: getShardCheckerHealthCheck,
		getShardAllMeta: getShardAllMeta

	}
}
