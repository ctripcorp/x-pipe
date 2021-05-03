/**  确认框 */
angular
    .module('directive')
    .directive('addshard', addshard);

function addshard($compile, $window) {
    return {
        restrict: 'E',
        templateUrl: '../../views/directives/add-shard.html',
        transclude: true,
        replace: true
        // TODO [marsqing] move shard create/delete opertion here
    }
}