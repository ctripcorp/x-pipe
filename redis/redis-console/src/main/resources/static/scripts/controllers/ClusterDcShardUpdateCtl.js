index_module.controller('ClusterDcShardUpdateCtl', ['$rootScope', '$scope', '$stateParams', '$window', '$location', 'toastr', 'AppUtil', 'ClusterService', 'ShardService',
    function ($rootScope, $scope, $stateParams, $window, $location, toastr, AppUtil, ClusterService, ShardService) {

        $scope.dcs, $scope.dcActiveTab, $scope.shards;
        $scope.clusterName = $stateParams.clusterName;
        $scope.shardName = $stateParams.shardName;
        
        $scope.switchDc = switchDc;
        $scope.loadCluster = loadCluster;
        $scope.loadShard = loadShard;
        
        $scope.preCreateRedis = preCreateRedis;
        $scope.createRedis = createRedis;
        $scope.preDeleteRedis = preDeleteRedis;
        $scope.deleteRedis = deleteRedis;

        $scope.submitUpdates = submitUpdates;


        if ($scope.clusterName) {
            loadCluster();
        }
        
        function switchDc(dc) {
            $scope.currentDcName = dc.dcName;
            loadShard($scope.clusterName, dc.dcName, $scope.shardName);
        }

        function loadCluster() {
            ClusterService.findClusterDCs($scope.clusterName)
                .then(function (result) {
                    if (!result || result.length == 0) {
                        $scope.dcs = [];
                        $scope.shards = [];
                        return;
                    }
                    $scope.dcs = result;
                    $scope.currentDcName = $scope.dcs[0].dcName;

                    loadShard($scope.clusterName, $scope.dcs[0].dcName, $scope.shardName);

                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });


        }

        function loadShard(clusterName, dcName, shardName) {
            ShardService.findClusterDcShard(clusterName, dcName, shardName)
                .then(function (result) {
                    $scope.shard = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }


        function preCreateRedis(type) {
            $scope.toCreateRedis = {};
            $scope.toCreateRedis.redisRole = type;

            $('#createRedisModal').modal('show');
        }

        function createRedis() {
        	if('redis' === $scope.toCreateRedis.redisRole) {
        		$scope.toCreateRedis.id = 0;
        		$scope.shard.redises.push($scope.toCreateRedis);
        		$scope.toCreateRedis = {};
        		$('#createRedisModal').modal('hide');
        	} 
        	if ('keeper' === $scope.toCreateRedis.redisRole){
        		$scope.toCreateRedis.id = 0;
        		$scope.shard.keepers.push($scope.toCreateRedis);
        		$scope.toCreateRedis = {};
        		$('#createRedisModal').modal('hide');
        	}
        }
        
        function preDeleteRedis(redis) {
        	$scope.toDeleteRedis = {};
            $scope.toDeleteRedis = redis;
            $('#deleteRedisConfirm').modal('show');
        }

        function deleteRedis() {
        	var index = -1;
        	for(var cnt_redis = 0 ; cnt_redis != $scope.shard.redises.length; ++cnt_redis) {
        		if($scope.toDeleteRedis == $scope.shard.redises[cnt_redis]) {
        			index = cnt_redis;
        			break;
        		}
        	}
        	if(index != -1) {
        		$scope.shard.redises.splice(index,1);
        		$scope.toDeleteRedis = {};
        		return;
        	}
        	
        	for(var cnt_keeper = 0; cnt_keeper != $scope.shard.keepers.length; ++cnt_keeper) {
        		if($scope.toDeleteRedis == $scope.shard.keepers[cnt_keeper]) {
        			index = cnt_keeper;
        			break;
        		}
        	}
        	if(index != -1) {
        		$scope.shard.keepers.splice(index,1);
        		$scope.toDeleteRedis = {};
        		return;
        	}
        	$scope.toDeleteRedis = {};
        }

        function submitUpdates() {
        	var toUpdateRedises = [];
        	for(var cnt_redis = 0 ; cnt_redis != $scope.shard.redises.length; ++cnt_redis) {
        		var proto = {};
        		var toUpdateRedis = $scope.shard.redises[cnt_redis];
        		proto.redisName = toUpdateRedis.id;
        		proto.redisIp = toUpdateRedis.ip;
        		proto.redisPort = toUpdateRedis.port;
        		// master check & set
        		
        		toUpdateRedises.push(proto);
        	}
        	for(var cnt_keeper = 0 ; cnt_keeper != $scope.shard.keepers.length; ++cnt_keeper) {
        		var proto = {};
        		var toUpdateRedis = $scope.shard.keepers[cnt_keeper];
        		proto.redisName = toUpdateRedis.id;
        		proto.redisIp = toUpdateRedis.ip;
        		proto.redisPort = toUpdateRedis.port;
        		// master check & set
        		proto.keeperContainerId = toUpdateRedis.keeperContainerId;
        		
        		toUpdateRedis.push(proto);
        	}
        	
        	// call service to update
        }
        
    }]);