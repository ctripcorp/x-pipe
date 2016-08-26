index_module.controller('ClusterDcCtl', ['$rootScope', '$scope', '$window','$stateParams', 'AppUtil', 'toastr', 'ClusterService','DcService',
    function ($rootScope, $scope, $window,$stateParams, AppUtil, toastr, ClusterService,DcService) {
	  
        $rootScope.currentNav = '1-4';
        
        $scope.clusterName = $stateParams.clusterName;
        
        $scope.unattached_dc = [];

		if ($scope.clusterName) {
			loadCluster();
		}

		$scope.preBindDc = preBindDc;
		$scope.bindDc = bindDc;
		$scope.preUnbindDc = preUnbindDc;
		$scope.unbindDc = unbindDc;

		$scope.toBindDc = {};
		function preBindDc(dc) {
			$scope.toBindDc = dc;
			$('#bindDcConfirm').modal('show');
		}

		function bindDc(dc) {
			ClusterService.bindDc($scope.clusterName, dc.dcName)
				.then(function (result) {
					toastr.success("bind success");
					$window.location.reload();
				}, function (result) {
					toastr.error("bind fail");
				});
		}

		$scope.toUnbindDc = {};
		function preUnbindDc(dc) {
			$scope.toUnbindDc = dc;
			$('#unbindDcConfirm').modal('show');
		}

		function unbindDc(dc) {
			ClusterService.unbindDc($scope.clusterName, dc.dcName)
				.then(function (result) {
					toastr.success("unbind success");
					$window.location.reload();
				}, function (result) {
					toastr.error("unbind fail");
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
