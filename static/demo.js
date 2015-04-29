// Generated by CoffeeScript 1.9.0
(function() {
  angular.module('project', []).controller('ServerStatusController', [
    '$scope', '$interval', function($scope, $interval) {
      var stop;
      $scope.servers = [];
      $scope.sending = false;
      $scope.init = function() {
        $.ajaxSetup({
          timeout: 2000
        });
        return $.get("/api/servers", function(data) {
          var address, addresses;
          addresses = JSON.parse(data);
          addresses.sort();
          $scope.servers = (function() {
            var _i, _len, _results;
            _results = [];
            for (_i = 0, _len = addresses.length; _i < _len; _i++) {
              address = addresses[_i];
              _results.push({
                address: address
              });
            }
            return _results;
          })();
          return $scope.$apply();
        });
      };
      $scope.restart = function(address) {
        return $.ajax({
          url: encodeURI("/api/servers/" + address + "/restart"),
          method: 'POST',
          success: function(data) {
            return alert(data);
          },
          error: function(xhr, ajaxOptions, err) {
            return alert(err);
          }
        });
      };
      $scope.start = function(address) {
        return $.ajax({
          url: encodeURI("/api/servers/" + address + "/start"),
          method: 'POST',
          success: function(data) {
            return alert(data);
          },
          error: function(xhr, ajaxOptions, err) {
            return alert(err);
          }
        });
      };
      $scope.stop = function(address) {
        return $.ajax({
          url: encodeURI("/api/servers/" + address + "/stop"),
          method: 'POST',
          success: function(data) {
            return alert(data);
          },
          error: function(xhr, ajaxOptions, err) {
            return alert(err);
          }
        });
      };
      $scope.send_client_command = function(address, command) {
        return $.ajax({
          url: encodeURI("/api/servers/" + address + "/command"),
          method: 'POST',
          contentType: 'application/json',
          data: JSON.stringify({
            'command': command
          }),
          success: function(data) {
            var resp;
            resp = JSON.parse(data);
            return alert("successfully sent command, reply message: '" + resp.message + "', request time: " + resp.time + "s");
          },
          error: function(xhr, ajaxOptions, err) {
            return alert(err);
          }
        });
      };
      $scope.updateStatus = function() {
        var i, _i, _ref, _results;
        _results = [];
        for (i = _i = 0, _ref = $scope.servers.length - 1; 0 <= _ref ? _i <= _ref : _i >= _ref; i = 0 <= _ref ? ++_i : --_i) {
          _results.push((function(i) {
            var address;
            address = $scope.servers[i].address;
            return $.ajax({
              url: encodeURI("/api/servers/" + address + "/status"),
              success: function(data) {
                var now;
                data = JSON.parse(data);
                now = new Date();
                $scope.servers[i].online = true;
                $scope.servers[i].leader = data.status;
                $scope.servers[i].leaderID = data.leaderID;
                $scope.servers[i].ping = data.msg || ("" + (now.toISOString()));
                return $scope.servers[i].pingTime = Math.round(Number(data.time) * 1000 * 1000) / 1000.0;
              },
              error: function(xhr, ajaxOptions, err) {
                $scope.servers[i].online = false;
                $scope.servers[i].leader = void 0;
                return $scope.servers[i].leaderID = void 0;
              }
            });
          })(i));
        }
        return _results;
      };
      stop = void 0;
      $scope.poll = function() {
        console.log('start polling');
        if (angular.isDefined(stop)) {
          console.log('stop already defined, return');
          return;
        }
        return stop = $interval(function() {
          return $scope.updateStatus();
        }, 1000);
      };
      $scope.stopPolling = function() {
        if (angular.isDefined(stop)) {
          $interval.cancel(stop);
          return stop = void 0;
        }
      };
      $scope.$on('destroy', function() {
        return $scope.stopPolling();
      });
      $scope.init();
      return $scope.poll();
    }
  ]);

}).call(this);
