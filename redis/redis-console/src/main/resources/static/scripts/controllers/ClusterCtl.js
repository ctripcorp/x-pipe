index_module.controller('ClusterCtl', ['$scope', '$window', 'ClusterService',
         function ($scope, $window, ClusterService) {

             $scope.clusterName = "";

             $scope.switchDc = switchDc;
             
             $scope.loadCluster = loadCluster;

             function switchDc(dc) {
                 $scope.dcActiveTab = dc.baseInfo.dcName;
             }

             function loadCluster() {
                 ClusterService.load_cluster($scope.clusterName)
                     .then(function (result) {
                         $scope.cluster = result;
                         if (result && result.dcs && result.dcs.length) {
                             $scope.dcActiveTab = result.dcs[0].baseInfo.dcName;
                         }
                     }, function (result) {

                     });
             }

             
             
             
         }]);
