/**  确认框 */
directive_module.directive('addshard', function ($compile, $window) {
    return {
        restrict: 'E',
        templateUrl: '../../views/directives/add-shard.html',
        transclude: true,
        replace: true
        // TODO [marsqing] move shard create/delete opertion here
    }
});
