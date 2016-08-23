index_module.controller('ClusterDcCtl', ['$rootScope', '$scope', '$window','$stateParams', 'AppUtil', 'toastr', 'ClusterService','DcService', 'SweetAlert',
    function ($rootScope, $scope, $window,$stateParams, AppUtil, toastr, ClusterService,DcService, SweetAlert) {
	  
        $rootScope.currentNav = '1-4';
        
        $scope.clusterName = $stateParams.clusterName;
        
        $scope.unattached_dc = [];

        if ($scope.clusterName) {
            loadCluster();            
        }
        
        function addDc(dc) {
        	
        }
        
        function deleteDc(dc) {
        	SweetAlert.swal({
     		   title: "Are you sure?",
     		   text: "Your will remove dc:" + dc.dcName + "!",
     		   type: "warning",
     		   showCancelButton: true,
     		   confirmButtonColor: "#DD6B55",confirmButtonText: "Yes, delete it!",
     		   cancelButtonText: "No, cancel plx!",
     		   closeOnConfirm: false,
     		   closeOnCancel: false }, 
     		function(isConfirm){ 
     		   if (isConfirm) {
     			   DcService.deleteCluster(dc.dcName)
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
        
        
        function loadCluster() {
        	ClusterService.load_cluster($scope.clusterName)
        		.then(function(result) {
        			$scope.cluster = result;
        		}, function(result) {
        			toastr.error(AppUtil.errorMsg(result));
        		});
        	
            ClusterService.findClusterDCs($scope.clusterName)
                .then(function (result) {
                    if (!result || result.length == 0) {
                        $scope.dcs = [];
                    }
                    $scope.dcs = result;
                    
                    if ($scope.unattached_dc) {
                    	loadDcs();
                    }
                    
                    
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }
        
        function loadDcs() {
        	DcService.loadAllDcs()
    		.then(function(result) {
    			var all_dcs = result;
    			for(var i = 0; i < all_dcs.length; i++) {
    				var dc = all_dcs[i];
    				var flag = false;
    				for(var j = 0 ; j < $scope.dcs.length; ++j) {
    					tmp_dc = $scope.dcs[j];
    					if(tmp_dc.id == dc.id) {
    						flag = true;
    					}
    				}
    				if(!flag) {
    					$scope.unattached_dc.push(dc);
    				}
    			}
    		}, function(result){
    			toastr.error(AppUtil.errorMsg(result));
    		});
        }
        
        
    }]);
