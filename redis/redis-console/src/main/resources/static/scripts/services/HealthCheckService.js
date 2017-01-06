services.service('HealthCheckService', ['$resource', '$q', function($resource, $q) {
	var resource = $resource('', {}, {
		is_redis_health: {
			method: 'GET',
			url: '/console/redis/health/:redisIp/:redisPort'
		},
		get_repl_delay: {
			method: 'GET',
			url: '/console/redis/delay/:redisIp/:redisPort'
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
	
	return {
		isRedisHealth : isRedisHealth,
		getReplDelay : getReplDelay
	}
}]);