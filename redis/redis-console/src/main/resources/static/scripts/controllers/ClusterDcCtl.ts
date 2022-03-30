angular
	.module('index')
	.controller('ClusterDcCtl', ClusterDcCtl);

ClusterDcCtl.$inject = [ '$rootScope', '$scope', '$window', '$stateParams', 'AppUtil',
		'toastr', 'ClusterService', 'DcService', 'ClusterType'];

function ClusterDcCtl($rootScope, $scope, $window, $stateParams, AppUtil,
					  toastr, ClusterService, DcService, ClusterType) {

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
	$scope.isActiveDc = isActiveDc;
	$scope.notActiveDc = notActiveDc;
	$scope.isCrossDc = isCrossDc;

	$scope.toBindDc = {};
	function preBindDc(dc) {
		$scope.toBindDc = dc;
		$('#bindDcConfirm').modal('show');
	}

	function bindDc() {
		ClusterService.bindDc($scope.clusterName,
				$scope.toBindDc.dcName).then(function(result) {
			toastr.success("绑定成功");
			$window.location.reload();
		}, function(result) {
			toastr.error("绑定失败");
		});
	}

	$scope.toUnbindDc = {};
	function preUnbindDc(dc) {
		$scope.toUnbindDc = dc;
		$('#unbindDcConfirm').modal('show');
	}

	function unbindDc() {
		ClusterService.unbindDc($scope.clusterName,
				$scope.toUnbindDc.dcName).then(function(result) {
			toastr.success("解绑成功");
			$window.location.reload();
		}, function(result) {
			toastr.error("解绑失败");
		});
	}

	function loadCluster() {
		ClusterService.load_cluster($scope.clusterName).then(
				function(result) {
					$scope.cluster = result;
				}, function(result) {
					toastr.error(AppUtil.errorMsg(result));
				});

		ClusterService.findClusterDCs($scope.clusterName).then(
				function(result) {
					if (!result || result.length == 0) {
						$scope.dcs = [];
					}
					$scope.dcs = result;

					if ($scope.unattached_dc) {
						loadDcs();
					}

				}, function(result) {
					toastr.error(AppUtil.errorMsg(result));
				});
	}

	function loadDcs() {
		DcService.loadAllDcs().then(function(result) {
			var all_dcs = result;
			for (var i = 0; i < all_dcs.length; i++) {
				var dc = all_dcs[i];
				var flag = false;
				for (var j = 0; j < $scope.dcs.length; ++j) {
					let tmp_dc = $scope.dcs[j];
					if (tmp_dc.id == dc.id) {
						flag = true;
					}
				}
				if (!flag) {
					$scope.unattached_dc.push(dc);
				}
			}
		}, function(result) {
			toastr.error(AppUtil.errorMsg(result));
		});
	}

	function isActiveDc(dcId) {
		if (!$scope.cluster) return false
		var clusterType = ClusterType.lookup($scope.cluster.clusterType)
		if(clusterType.isCrossDc)
			return false
		if (clusterType && clusterType.multiActiveDcs) return true
		else return $scope.cluster.activedcId === dcId
	}

	function notActiveDc(dcId) {
		if (!$scope.cluster) return false
		var clusterType = ClusterType.lookup($scope.cluster.clusterType)
		if(clusterType.isCrossDc)
			return false
		else return !isActiveDc(dcId);
	}

	function isCrossDc() {
		var clusterType = ClusterType.lookup($scope.cluster.clusterType)
		return clusterType.isCrossDc;
	}
}
