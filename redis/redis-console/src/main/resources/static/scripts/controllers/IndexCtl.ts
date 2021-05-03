angular
    .module('index')
    .controller('IndexCtl', IndexCtl);

IndexCtl.$inject = ['$rootScope', '$scope', '$window', 'UserService', 'ConfigService', 'AppUtil'];

function IndexCtl($rootScope, $scope, $window, UserService, ConfigService, AppUtil) {

    $rootScope.currentNav = '1-2';
    $rootScope.switchNav = switchNav;
    $rootScope.historyPage = 1;
    $rootScope.preChangeConfig = preChangeConfig;
    $rootScope.changeConfig = changeConfig;
    $rootScope.updateConfig = updateConfig;

    function switchNav(nav) {
        $rootScope.currentNav = nav;
    }

    UserService.getCurrentUser()
        .then(function (result) {
            $rootScope.currentUser = result;
        });
    ConfigService.isAlertSystemOn()
        .then(function (result) {
            $rootScope.isAlertSystemOn = (result.state === 0);
        });
    ConfigService.isSentinelAutoProcessOn()
        .then(function (result) {
            $rootScope.isSentinelAutoProcOn = (result.state === 0);
        });

    function preChangeConfig(key, value) {
        $rootScope.configChangeKey = key;
        $rootScope.configChangeValue = value;
        $('#changeConfigConfirm').modal('show');
    }

    function changeConfig() {
        ConfigService.changeConfig($rootScope.configChangeKey, $rootScope.configChangeValue)
            .then(function (result) {
                $window.location.reload();
                $('#changeConfigConfirm').modal('hide');
                toastr.success('设置更改成功');
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '设置更改失败');
                $window.location.reload();
            })
    }

    function updateConfig() {
        ConfigService.isAlertSystemOn()
            .then(function (result) {
                $rootScope.isAlertSystemOn = (result.state === 0);
            });
        ConfigService.isSentinelAutoProcessOn()
            .then(function (result) {
                $rootScope.isSentinelAutoProcOn = (result.state === 0);
            });
        $window.location.reload();
    }
}
