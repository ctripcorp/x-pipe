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

    return {
        findAll: findAll,
        findById: findById,
        create: create,
        update: update,
        remove: remove
    };
}
