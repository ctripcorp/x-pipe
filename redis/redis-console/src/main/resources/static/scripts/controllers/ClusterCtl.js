index_module.controller('ClusterCtl', ['$scope', '$stateParams', '$window', '$location', 'AppUtil', 'ClusterService', 'ShardService',
         function ($scope, $stateParams, $window, $location, AppUtil, ClusterService, ShardService) {

             $scope.clusterName = $stateParams.clusterName;

             $scope.switchDc = switchDc;
             
             $scope.loadCluster = loadCluster;
             $scope.loadShards = loadShards;

             function switchDc(dc) {
                 $scope.dcActiveTab = dc.baseInfo.dcName;
             }

             function loadCluster() {
                 ClusterService.findClusterDCs($scope.clusterName)
                     .then(function (result) {
                         $scope.dcs = result;
                        
                         loadShards($scope.clusterName, $scope.dcs[0].dcName);
                         
                     }, function (result) {
                        toastr.error(AppUtil.errorMsg());    
                     });
                 
                 
             }
             
             function loadShards(clusterName, dcName) {
                 ShardService.findShards(clusterName, dcName)
                     .then(function (result) {
                         $scope.shards = result;
                     },function (result) {
                         toastr.error(AppUtil.errorMsg());
                     });    
             }

             
             
             
         }]);
