angular
    .module('index')
    .controller('FullLinkHealthCheckCtl', FullLinkHealthCheckCtl)
    .config(function($mdThemingProvider) {
        $mdThemingProvider.theme('green').backgroundPalette('light-green');
        $mdThemingProvider.theme('red').backgroundPalette('red');
        $mdThemingProvider.theme('orange').backgroundPalette('orange');
    });

FullLinkHealthCheckCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'HealthCheckService',
    'toastr', 'NgTableParams', 'AppUtil', '$interval'];

function FullLinkHealthCheckCtl($rootScope, $scope, $window, $stateParams, HealthCheckService,
                                   toastr, NgTableParams, $interval) {
    $scope.masterRoles = [];
    $scope.slaveRoles = [];
    $scope.unknownRoles = [];
    $scope.shardCheckerHealthCheckResult = [];
    $scope.showActions=false;
    $scope.shardAllMeta = null;
    $scope.shardKeeperState = [];

    $scope.redisRoleHealthCheck = redisRoleHealthCheck;
    $scope.shardCheckerGroupHealthCheck = shardCheckerGroupHealthCheck;
    $scope.doShowActions = doShowActions;
    $scope.getShardAllMeta = getShardAllMeta;
    $scope.getShardKeeperState = getShardKeeperState;

    redisRoleHealthCheck();
    shardCheckerGroupHealthCheck();
    getShardAllMeta();
    getShardKeeperState();

    function redisRoleHealthCheck() {
        HealthCheckService.getShardRedisRole($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.masterRoles = [];
            $scope.slaveRoles = [];
            $scope.unknownRoles = [];
            response.forEach(function (item){
                if (item.role == null) {
                    $scope.unknownRoles.push(item)
                }else if (item.role.serverRole == "SLAVE") {
                    $scope.slaveRoles.push(item);
                } else if (item.role.serverRole == "MASTER") {
                    $scope.masterRoles.push(item);
                }
            })
        })
    }

    function shardCheckerGroupHealthCheck() {
        HealthCheckService.getShardCheckerHealthCheck($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.shardCheckerHealthCheckResult = [];
            response.forEach(function (item){
                $scope.shardCheckerHealthCheckResult.push(item);
            })
        })
    }

    function getShardAllMeta() {
        HealthCheckService.getShardAllMeta($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.shardAllMeta = null;
            $scope.shardAllMeta = response;
        })
    }

    function getShardKeeperState() {
        HealthCheckService.getShardKeeperState($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.shardKeeperState = [];
            response.forEach(function (item){
                $scope.shardKeeperState.push(item);
            })
        })
    }

    function doShowActions() {
        $scope.showActions = !$scope.showActions;
    }


}