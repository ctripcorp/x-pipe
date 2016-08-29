index_module.controller('ClusterCtl', ['$rootScope', '$scope', '$stateParams', '$window', '$location', 'toastr', 'AppUtil', 'ClusterService', 'ShardService','SweetAlert',
    function ($rootScope, $scope, $stateParams, $window, $location, toastr, AppUtil, ClusterService, ShardService, SweetAlert) {

        $scope.dcs, $scope.dcActiveTab, $scope.shards;
        $scope.clusterName = $stateParams.clusterName;
        
        $scope.switchDc = switchDc;
        $scope.loadCluster = loadCluster;
        $scope.loadShards = loadShards;
        $scope.preCreateRedis = preCreateRedis;
        $scope.createRedis = createRedis;
        $scope.preDeleteRedis = preDeleteRedis;
        $scope.deleteRedis = deleteRedis;



        if ($scope.clusterName) {
            loadCluster();
        }
        
        function switchDc(dc) {
            $scope.currentDcName = dc.dcName;
            loadShards($scope.clusterName, dc.dcName);
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

                    loadShards($scope.clusterName, $scope.dcs[0].dcName);

                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });


        }

        function loadShards(clusterName, dcName) {
            ShardService.findClusterDcShards(clusterName, dcName)
                .then(function (result) {
                    $scope.shards = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }


        $scope.toCreateRedisInShard = {};
        function preCreateRedis(shard, type) {
            $scope.toCreateRedisInShard = shard;
            $scope.toCreateRedis = {};
            $scope.toCreateRedis.redisRole = type;

            $('#createRedisModal').modal('show');
        }


        function createRedis() {
            ShardService.bindRedis($scope.clusterName, $scope.currentDcName,
                                   $scope.toCreateRedisInShard.id, $scope.toCreateRedis)
                .then(function (result) {
                    toastr.success('create redis success');     
                    $window.location.reload();
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), 'create redis fail');
                });
        }

        $scope.toDeleteRedis = {};
        $scope.toDeleteRedisInShard = {};
        function preDeleteRedis(shard, redis) {
            $scope.toDeleteRedisInShard = shard;
            $scope.toDeleteRedis = redis;
            $('#deleteRedisConfirm').modal('show');
        }

        function deleteRedis() {
            ShardService.unbindRedis($scope.clusterName, $scope.currentDcName,
                                     $scope.toDeleteRedisInShard.id, $scope.toDeleteRedis.id)
                .then(function (result) {
                    toastr.success('delete success');
                    $window.location.reload();
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), 'delete fail');
                });

        }



    }]);