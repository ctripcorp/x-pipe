index_module.controller('IndexCtl', ['$rootScope', '$scope',
    function ($rootScope, $scope) {

        $rootScope.currentNav = '';
        $rootScope.switchNav = switchNav;
        
        function switchNav(nav) {
            $rootScope.currentNav = nav;
        }

    }]);
