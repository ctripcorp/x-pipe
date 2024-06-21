angular
    .module('services')
    .service('ConfigService', ConfigService);

ConfigService.$inject = ['$resource', '$q'];

function ConfigService($resource, $q) {

    var resource = $resource('', {}, {
        is_alert_system_on: {
            method: 'GET',
            url: '/console/config/alert_system'
        },
        is_sentinel_auto_process_on: {
            method: 'GET',
            url: '/console/config/sentinel_auto_process'
        },
        is_keeper_balance_info_collect_on: {
            method: 'GET',
            url: '/console/config/keeper_balance_info_collect'
        },
        change_config: {
            method: 'POST',
            url: '/console/config/change_config'
        }
    });

    function isAlertSystemOn() {
        var d = $q.defer();
        resource.is_alert_system_on({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function isSentinelAutoProcessOn() {
        var d = $q.defer();
        resource.is_sentinel_auto_process_on({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function isKeeperBalanceInfoCollectOn() {
        var d = $q.defer();
        resource.is_keeper_balance_info_collect_on({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function changeConfig(key, value) {
        var d = $q.defer();
        resource.change_config({}, {
                key: key,
                val: value
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        isAlertSystemOn: isAlertSystemOn,
        isSentinelAutoProcessOn: isSentinelAutoProcessOn,
        isKeeperBalanceInfoCollectOn: isKeeperBalanceInfoCollectOn,
        changeConfig: changeConfig
    }
}