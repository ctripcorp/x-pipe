index_module.controller('IndexCtl', ['$rootScope', '$scope', 'UserService',
    function ($rootScope, $scope, UserService) {

        $rootScope.currentNav = '1-2';
        $rootScope.switchNav = switchNav;
        $rootScope.historyPage = 1;
        
        function switchNav(nav) {
            $rootScope.currentNav = nav;
        }
        
        UserService.getCurrentUser()
            .then(function (result) {
                $rootScope.currentUser = result; 
            })

    }]);
