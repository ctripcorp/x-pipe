services.service('HealthCheckService', ['$resource', '$q', function($resource, $q) {
	var resource = $resource('', {}, {
		is_redis_health: {
			method: 'GET',
			url: '/console/redis/health/:redisIp/:redisPort'
		},
		get_repl_delay: {
			method: 'GET',
			url: '/console/redis/delay/:redisIp/:redisPort'
		},
		get_hickwall_addr: {
			method: 'GET',
			url: '/console/redis/health/hickwall/:cluster/:shard/:redisIp/:redisPort'
		},
		get_cross_master_delay: {
			method: 'GET',
			url: '/console/cross-master/delay/:dc/:cluster/:shard'
		},
		get_cross_master_hickwall_addr: {
			method: 'GET',
			url: '/console/cross-master/health/hickwall/:cluster/:shard/:sourceDc/:destDc'
		}
	});
	
	function isRedisHealth(ip,port) {
		var d = $q.defer();
		resource.is_redis_health({
			redisIp : ip,
			redisPort: port
		},
		function(result) {
			d.resolve(result);
		}, function(result) {
			d.reject(result);
		});
		return d.promise;
	}
	
	function getReplDelay(ip, port) {
		var d = $q.defer();
		resource.get_repl_delay({
			redisIp : ip,
			redisPort : port
		}, function(result) {
			d.resolve(result);
		}, function(result) {
			d.reject(result);
		});
		return d.promise;
	}
	
	function getHickwallAddr(cluster, shard, redisIp, redisPort) {
		var d = $q.defer();
		resource.get_hickwall_addr({
			cluster : cluster,
			shard : shard,
			redisIp : redisIp,
			redisPort : redisPort
		},
				function(result) {
			d.resolve(result);
		}, function(result) {
			d.reject(result);
		});
		return d.promise;
	}

	function getCrossMasterDelay(dc, cluster, shard) {
		var d = $q.defer();
		resource.get_cross_master_delay({
				dc, cluster, shard,
			},
			function(result) {
				d.resolve(result);
			}, function(result) {
				d.reject(result);
			});
		return d.promise;
	}

	function getCrossMasterHickwallAddr(cluster, shard, sourceDc, destDc) {
		var d = $q.defer();
		resource.get_cross_master_hickwall_addr({
				cluster, shard, sourceDc, destDc
			},
			function(result) {
				d.resolve(result);
			}, function(result) {
				d.reject(result);
			});
		return d.promise;
	}
	
	return {
		isRedisHealth : isRedisHealth,
		getReplDelay : getReplDelay,
		getHickwallAddr : getHickwallAddr,
		getCrossMasterDelay : getCrossMasterDelay,
		getCrossMasterHickwallAddr, getCrossMasterHickwallAddr
	}
}]);