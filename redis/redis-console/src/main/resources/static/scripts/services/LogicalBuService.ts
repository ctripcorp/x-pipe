angular
    .module('services')
    .service('LogicalBuService', LogicalBuService);

LogicalBuService.$inject = ['$resource', '$q'];

function LogicalBuService($resource, $q) {

    var resource = $resource('', {}, {
        find_all: {
            method: 'GET',
            url: '/console/logical-bus',
            isArray: true
        },
        find_by_id: {
            method: 'GET',
            url: '/console/logical-bus/:id'
        },
        create: {
            method: 'POST',
            url: '/console/logical-bus'
        },
        update: {
            method: 'PUT',
            url: '/console/logical-bus/:id'
        },
        remove: {
            method: 'DELETE',
            url: '/console/logical-bus/:id'
        },
        keeper_state: {
            method: 'POST',
            url: '/console/logical-bus/keeperState',
            timeout: 90000
        },
        reset_health: {
            method: 'POST',
            url: '/console/logical-bus/resetHealth',
            timeout: 150000
        },
        switch_restore: {
            method: 'POST',
            url: '/console/logical-bus/switchRestore',
            timeout: 150000
        },
        switch_bm: {
            method: 'POST',
            url: '/console/logical-bus/switchBM',
            // server side is synchronous: precheck + DB + MetaServer notify + election verify
            timeout: 150000
        }
    });

    function findAll() {
        var d = $q.defer();
        resource.find_all({}, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function findById(id) {
        var d = $q.defer();
        resource.find_by_id({id: id}, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function create(model) {
        var d = $q.defer();
        resource.create({}, model, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function update(id, model) {
        var d = $q.defer();
        resource.update({id: id}, model, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function remove(id) {
        var d = $q.defer();
        resource.remove({id: id}, function () {
            d.resolve();
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function keeperState(logicalBuIds) {
        var d = $q.defer();
        resource.keeper_state({}, {logicalBuIds: logicalBuIds}, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function resetHealth(logicalBuIds) {
        var d = $q.defer();
        resource.reset_health({}, {logicalBuIds: logicalBuIds}, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function switchRestore(logicalBuIds) {
        var d = $q.defer();
        resource.switch_restore({}, {logicalBuIds: logicalBuIds}, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function switchBM(logicalBuIds) {
        var d = $q.defer();
        resource.switch_bm({}, {logicalBuIds: logicalBuIds}, function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    return {
        findAll: findAll,
        findById: findById,
        create: create,
        update: update,
        remove: remove,
        switchBM: switchBM,
        switchRestore: switchRestore,
        resetHealth: resetHealth,
        keeperState: keeperState
    };
}
