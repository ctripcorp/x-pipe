angular
    .module('services')
    .service('UserService', UserService);

UserService.$inject = ['$resource', '$q'];

function UserService($resource, $q) {

    var resource = $resource('', {}, {
        get_current_user: {
            method: 'GET',
            url: '/console/user/current'
        },
        logout: {
            method: 'GET',
            url: '/console/user/logout'
        }
    });

    function getCurrentUser() {
        var d = $q.defer();
        resource.get_current_user({},
                                  function (result) {
                                      d.resolve(result);
                                  }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function logout() {
        var d = $q.defer();
        resource.logout({},
                        function (result) {
                            d.resolve(result);
                        }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        getCurrentUser: getCurrentUser,
        logout: logout
    }
}