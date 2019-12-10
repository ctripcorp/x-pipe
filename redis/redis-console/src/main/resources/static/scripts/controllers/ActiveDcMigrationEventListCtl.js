index_module.controller('ActiveDcMigrationEventListCtl', [
    '$rootScope', '$scope', '$window', '$stateParams', 'AppUtil',
    'toastr', 'NgTableParams', 'MigrationService', '$q',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, MigrationService, $q) {
        $scope.page = 1
        $scope.size = 10
        $scope.clusterName = $stateParams.clusterName

		$scope.tableParams = new NgTableParams({
            page : $scope.page,
            count : $scope.size,
        }, {
            counts: [10, 25, 50],
            getData : function(params) {
                $scope.page = params.page()
                $scope.size = params.count()

                var deferred = $q.defer()
                MigrationService.find($scope.page - 1, $scope.size, $scope.clusterName)
                    .then(function (response) {
                        if (response.totalSize >= 0) params.total(response.totalSize)
                        deferred.resolve(response.data)
                    })
                    .catch(function (err) {
                        deferred.reject(err)
                    })

                return deferred.promise
            },
        });

        $scope.refresh = function() {
            $scope.tableParams.page(1)
            $scope.tableParams.reload()
        }

        $scope.clusterBlock = {
            "max-width": "200px",
            "text-overflow": "ellipsis",
            "white-space": "nowrap",
            "overflow": "hidden"
        }
    }]);