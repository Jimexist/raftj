angular.module('project', [])

.controller('ServerStatusController', ['$scope', '$interval', ($scope, $interval) -> 

  $scope.servers = []
  
  $scope.init = () ->
    $.get "/api/servers", (data) ->
      $scope.servers = ({
        address: address,
        ping: '',
        pingDelay: -1,
        status: '',
        leaderID: ''
      } for address in JSON.parse(data))
      $scope.$apply()
  
  $scope.restart = (address) ->
    console.log "restarting #{address}"
    $.ajax
      url: encodeURI("/api/servers/#{address}/start")
      method: 'POST'
      success: (data) -> 
        console.log data
      error: (err) -> 
        alert err
  
  $scope.stop = (address) ->
    console.log "stopping #{address}"
    $.ajax
      url: encodeURI("/api/servers/#{address}/stop")
      method: 'POST'
      success: (data) -> 
        console.log data
      error: (err) -> 
        alert err
  
  $scope.updateStatus = () ->
    for i in [0..($scope.servers.length-1)]
      do (i) ->
        address = $scope.servers[i].address
        $.ajax
          url: encodeURI("/api/servers/#{address}/status")
          success: (data) ->
            data = JSON.parse(data)
            $scope.servers[i].status = data.status
            $scope.servers[i].leaderID = data.leaderID
            $scope.servers[i].ping = data.msg or (new Date()).toTimeString()
            $scope.servers[i].pingTime = Math.round(Number(data.time) * 1000 * 1000) / 1000.0
          error: (err) -> 
            console.log err
  
  stop = undefined
  
  $scope.poll = () ->
    console.log 'start polling'
    if angular.isDefined(stop)
      console.log 'stop already defined, return'
      return
    stop = $interval(() -> 
      $scope.updateStatus()
    , 1000)
    
  $scope.stopPolling = () ->
    if angular.isDefined(stop)
      $interval.cancel stop
      stop = undefined
  
  $scope.$on('destroy', () -> $scope.stopPolling())
  
  $scope.init()
  $scope.poll()
  
])

.controller('ClientController', ['$scope', () ->

  client = this
  
])