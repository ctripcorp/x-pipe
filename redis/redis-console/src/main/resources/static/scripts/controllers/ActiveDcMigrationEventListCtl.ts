angular
    .module('index')
    .controller('ActiveDcMigrationEventListCtl', ActiveDcMigrationEventListCtl);

ActiveDcMigrationEventListCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil',
    'toastr', 'NgTableParams', 'MigrationService', '$q', 'ClusterType'];

function ActiveDcMigrationEventListCtl($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, MigrationService, $q, ClusterType) {
    $scope.page = 1
    $scope.size = 10
    $scope.clusterName = $stateParams.clusterName
    $scope.operator = undefined;
    $scope.status = undefined;
    $scope.statuses = ["Initiated", "Checking", "CheckingFail", "Migrating", "PartialSuccess", "PartialRetryFail",
        "Publish", "PublishFail", "RollBack", "RollBackFail",
        "Aborted", "Success", "ForceEnd"];

    $scope.onClusterChange = function() {
        $scope.operator = undefined;
        $scope.status = undefined;
        $scope.refresh();
    }

    $scope.onOperatorChange = function() {
        $scope.clusterName = undefined;
        $scope.status = undefined;
        $scope.refresh();
    }

    $scope.onStatusChange = function(status) {
        console.log("ActiveDcMigrationEventListCtl: onStatusChange", status);
        $scope.clusterName = undefined;
        $scope.operator = undefined;
        $scope.refresh();
    }

	$scope.tableParams = new NgTableParams({
        page : $scope.page,
        count : $scope.size,
    }, {
        counts: [10, 25, 50],
        getData : function(params) {
            $scope.page = params.page()
            $scope.size = params.count()
            let deferred = $q.defer()

            let promise;
            if (!!$scope.status) {
                promise = MigrationService.findByMigrationStatus($scope.page - 1, $scope.size, $scope.status);
            } else if (!!$scope.operator) {
                promise = MigrationService.findByOperator($scope.page - 1, $scope.size, $scope.operator);
            } else {
                promise = MigrationService.find($scope.page - 1, $scope.size, $scope.clusterName)
            }
            promise.then(function (response) {
                if (response.totalSize >= 0) params.total(response.totalSize)
                deferred.resolve(response.data)
            }).catch(function (err) {
                deferred.reject(err)
            });
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
}