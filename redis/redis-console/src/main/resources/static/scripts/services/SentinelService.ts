angular
	.module('services')
	.service('SentinelService', SentinelService);

SentinelService.$inject = ['$resource', '$q'];

function SentinelService($resource, $q) {
	var resource = $resource('', {}, {
		find_sentinels_by_dc: {
			method: 'GET',
			url: '/console/:dcName/sentinels',
			isArray: true
		},
		find_sentinels_by_dc_type: {
			method: 'GET',
			url: '/console/:dcName/:clusterType/sentinels',
			isArray: true
		},
		find_sentinel_by_id: {
			method: 'GET',
			url: '/console/sentinels/:sentinelId'
		},
		find_sentinels_by_shard: {
			method: 'GET',
			url: '/console/sentinels/shard/:shardId'
		}
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

	function findSentinelsByDcAndType(dcName, clusterType) {
		var d = $q.defer();
		resource.find_sentinels_by_dc_type({
				dcName: dcName,
				clusterType: clusterType
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
		return d.promise;
	}
	
	function findSentinelsByShardId(shardId) {
		var d = $q.defer();
		resource.find_sentinels_by_shard({
			shardId : shardId
		}, function(result) {
			d.resolve(result);
		}, function(result) {
			d.reject(result);
		});
		return d.promise;
	}
	
	return {
		findSentinelsByDc: findSentinelsByDc,
		findSentinelsByDcAndType: findSentinelsByDcAndType,
		findSentinelById: findSentinelById,
		findSentinelsByShardId: findSentinelsByShardId
	}
}