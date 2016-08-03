index_module.controller('ClusterCtl', ['$scope', '$window', 'ClusterService',
         function ($scope, $window, ClusterService) {

             $scope.clusterName = "cluster01";

             $scope.switchDc = switchDc;

             function switchDc(dc) {
                 $scope.dcActiveTab = dc.baseInfo.dcId;
             }

             ClusterService.load_cluster($scope.clusterName)
                 .then(function (result) {
                     $scope.cluster = result;
                     if (result && result.dcs && result.dcs.length) {
                         $scope.dcActiveTab = result.dcs[0].baseInfo.dcId;
                     }
                 }, function (result) {

                 });
             
             
         }]);
