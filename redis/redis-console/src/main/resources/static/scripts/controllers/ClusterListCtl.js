index_module.controller('ClusterListCtl', ['$rootScope', '$scope', '$window', 'AppUtil', 'toastr', 'ClusterService', 'SweetAlert','NgTableParams',
    function ($rootScope, $scope, $window, AppUtil, toastr, ClusterService, SweetAlert, NgTableParams, $filters) {

        $rootScope.currentNav = '1-2';
        
        $scope.deleteCluster = deleteCluster;


        $scope.tableParams = new NgTableParams({
            page : 1,
            count : 10
        }, {
        	filterDelay:100,
            getData : function(params) {
                return ClusterService.findAllClusters()
                    .then(function(data) {
                    var filter_text = params.filter().clusterName;
                    if(undefined !== filter_text && "" !== filter_text) {
                    	var filtered_data = new Array();
                    	for(var i = 0 ; i < data.length ; i++) {
                    		var cluster = data[i];
                    		if(cluster.clusterName.search(filter_text) != -1) {
                    			filtered_data.push(cluster);
                    		}
                    	}
                    	data = filtered_data;
                    }
                    
                    params.total(data.length);
                    return data.slice((params.page() - 1) * params.count(), params.page() * params.count());
                    });
            }
        });


        function deleteCluster(cluster) {
        	SweetAlert.swal({
        		   title: "Are you sure?",
        		   text: "Your will delete cluster:" + cluster.clusterName + "!",
        		   type: "warning",
        		   showCancelButton: true,
        		   confirmButtonColor: "#DD6B55",confirmButtonText: "Yes, delete it!",
        		   cancelButtonText: "No, cancel plx!",
        		   closeOnConfirm: false,
        		   closeOnCancel: false }, 
        		function(isConfirm){ 
        		   if (isConfirm) {
        			   ClusterService.deleteCluster(cluster.clusterName)
                       .then(function (result) {
                           location.reload(true);
                       }, function (result) {
                           toastr.error(AppUtil.errorMsg(result));
                       })
        		   } else {
        		      SweetAlert.swal("Cancelled", "Delete cancelled :)", "error");
        		   }
        		});
        }

        function findAllClusters() {
            ClusterService.findAllClusters()
                .then(function (result) {
                    $scope.clusters = result;
                    return result;
                }, function (result) {

                });
        }

    }]);
