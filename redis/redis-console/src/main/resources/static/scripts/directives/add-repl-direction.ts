/**  确认框 */
angular
    .module('directive')
    .directive('addrepldirction', addrepldirction);

function addrepldirction($compile, $window) {
    return {
        restrict: 'E',
        templateUrl: '../../views/directives/add-repl-direction.html',
        transclude: true,
        replace: true
    }
}