index_module.controller('ActiveDcMigrationEventListCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, $filters) {

		$scope.tableParams = new NgTableParams({
            page : 1,
            count : 10
        }, {
            filterDelay:100,
            getData : function(params) {
                params.total(2);
                return [{
    				'id' : 2,
    				'startTime' : '2016-09-12-22:18:32',
    				'endTime' : '2016-09-22-22:18:31',
    				'status' : 'success'
    		},
    		{
    			'id' : 1,
				'startTime' : '2016-09-13-22:18:32',
				'endTime' : '2016-09-17-22:18:31',
				'status' : 'fail'
    		}];
            }
        });
    }]);