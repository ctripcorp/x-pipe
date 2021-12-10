angular
	.module('services')
	.service('HealthCheckService', HealthCheckService);

HealthCheckService.$inject = ['$resource', '$q'];

function HealthCheckService($resource, $q) {
	const IS_REDIS_HEALTH = "is_redis_health";
	const GET_REPL_DELAY = "get_repl_delay";
	const GET_HICKWALL_ADDR = "get_hickwall_addr";
	const GET_CROSS_MASTER_DELAY = "get_cross_master_delay";
	const GET_CROSS_MASTER_HICKWALL_ADDR = "get_cross_master_hickwall_addr";
	const GET_PEER_OUTCOMING = "get_peer_outcoming";
	const GET_PEER_INCOMING = "get_peer_incoming";
	const GET_PEER_FULL_SYNC = "get_peer_full_sync";
	const GET_PEER_PARTIAL_SYNC = "get_peer_partial_sync";

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
		apis[GET_HICKWALL_ADDR] = {
			method: 'GET',
			url: '/console/redis/health/hickwall/:cluster/:shard/:redisIp/:redisPort'
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
	
	function getHickwallAddr(cluster, shard, redisIp, redisPort) {
		return request($q.defer(), GET_HICKWALL_ADDR, {
			cluster : cluster,
			shard : shard,
			redisIp : redisIp,
			redisPort : redisPort
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

	return {
		isRedisHealth : isRedisHealth,
		getReplDelay : getReplDelay,
		getHickwallAddr : getHickwallAddr,
		getCrossMasterDelay : getCrossMasterDelay,
		getCrossMasterHickwallAddr: getCrossMasterHickwallAddr,
		getOutComingTrafficToPeerHickwallAddr: getOutComingTrafficToPeerHickwallAddr,
		getInComingTrafficFromPeerHickwallAddr: getInComingTrafficFromPeerHickwallAddr,
		getPeerSyncFullHickwallAddr: getPeerSyncFullHickwallAddr,
		getPeerSyncPartialHickwallAddr: getPeerSyncPartialHickwallAddr

	}
}
