index_module.controller('ClusterListCtl', ['$rootScope', '$scope', '$window', 'AppUtil', 'toastr', 'ClusterService','DcService', 'NgTableParams',
    function ($rootScope, $scope, $window, AppUtil, toastr, ClusterService, DcService, NgTableParams, $filters) {

        $rootScope.currentNav = '1-2';
        $scope.dcs = {}
        
        $scope.getDcName = getDcName;
        
        var sourceClusters = [], copedClusters = [];
        ClusterService.findAllClusters()
            .then(function(data) {
                sourceClusters = data;
                copedClusters = _.clone(sourceClusters);
                $scope.tableParams = new NgTableParams({
                    page : 1,
                    count : 10
                }, {
                    filterDelay:100,
                    getData : function(params) {
                        var filter_text = params.filter().clusterName;
                        if(filter_text) {
                            var filtered_data = new Array();
                            for(var i = 0 ; i < copedClusters.length ; i++) {
                                var cluster = copedClusters[i];
                                if(cluster.clusterName.search(filter_text) != -1) {
                                    filtered_data.push(cluster);
                                }
                            }
                            copedClusters = filtered_data;
                        }else {
                            copedClusters = sourceClusters;
                        }

                        params.total(copedClusters.length);
                        return copedClusters.slice((params.page() - 1) * params.count(), params.page() * params.count());
                    }
                });
            });

        DcService.loadAllDcs()
        	.then(function(data) {
        		for(var i = 0 ; i < data.length; ++i) {
        			var dc = data[i];
        			$scope.dcs[dc.id] = dc.dcName;
        		}
        	});
        
        function getDcName(dcId) {
        	return $scope.dcs[dcId] || "Unbind";
        }
    }]);
