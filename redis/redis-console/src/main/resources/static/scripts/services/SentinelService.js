services.service('SentinelService', ['$resource', '$q', function ($resource, $q) {
	var resource = $resource('', {}, {
		find_sentinels_by_dc: {
			method: 'GET',
			url: '/console/:dcName/sentinels',
			isArray: true
		},
		find_sentinel_by_id: $resource('', {}, {
			method: 'GET',
			url: '/console/sentinels/:sentinelId'
		})
	});
	
	function findSentinelsByDc(dcName) {
		var d = $q.defer();
		resource.find_sentinels_by_dc({
			dcName : dcName
		},
				function(result) {	
			d.resolve(result);
		}, function(result) {
			d.reject(result);
		});
		return d.promise;
	}
	
	function findSentinelById(sentinelId) {
		var d = $q.defer();
		resource.find_sentinel_by_id({
			sentinelId : sentinelId
		},function(result) {
			d.resolve(result);
		}, function(result) {
			d.reject(result);
		});
	}
	
	
	
	return {
		findSentinelsByDc: findSentinelsByDc,
		findSentinelById: findSentinelById
	}
}]);