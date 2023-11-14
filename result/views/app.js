var app = angular.module('catsvsdogs', []);
var socket = io.connect();

app.controller('statsCtrl', function ($scope) {
  $scope.distanciaManhattan = 0;
  $scope.distanciaPerson = 0;

  var updateDistances = function () {
    socket.on('distances', function (json) {
      var data = JSON.parse(json);
      $scope.$apply(function () {
        $scope.distanciaManhattan = data.distancia_manhattan;
        $scope.distanciaPerson = data.distancia_pearson;
      });
    });
  };

  var init = function () {
    document.body.style.opacity = 1;
    updateDistances();
  };

  var resetValues = function () {
    $scope.distanciaManhattan = 0;
    $scope.distanciaPerson = 0;
  };

  socket.on('message', function (data) {
    resetValues();
    init();
  });
});