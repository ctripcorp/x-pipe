index_module.controller('IndexCtl', ['$rootScope', '$scope',
    function ($rootScope, $scope) {

        $rootScope.currentNav = '1-2';
        $rootScope.switchNav = switchNav;
        $rootScope.historyPage = 1;
        
        function switchNav(nav) {
            $rootScope.currentNav = nav;
        }

    }]);
