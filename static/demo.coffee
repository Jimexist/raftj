angular.module('project', [])

.controller('ServerStatusController', ['$scope', '$interval', ($scope, $interval) -> 

  $scope.servers = []
  
  $scope.init = () ->
    $.get "/api/servers", (data) ->
      $scope.servers = ({
        address: address,
        ping: '',
        pingDelay: -1,
        leader: false,
        online: false,
        leaderID: ''
      } for address in JSON.parse(data))
      $scope.$apply()
  
  $scope.restart = (address) ->
    $.ajax
      url: encodeURI("/api/servers/#{address}/restart")
      method: 'POST'
      success: (data) -> 
        alert data
      error: (err) -> 
        alert err
  
  $scope.start = (address) ->
    $.ajax
      url: encodeURI("/api/servers/#{address}/start")
      method: 'POST'
      success: (data) -> 
        alert data
      error: (err) -> 
        alert err
  
  $scope.stop = (address) ->
    $.ajax
      url: encodeURI("/api/servers/#{address}/stop")
      method: 'POST'
      success: (data) -> 
        alert data
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
            $scope.servers[i].online = true
            $scope.servers[i].leader = data.status
            $scope.servers[i].leaderID = data.leaderID
            $scope.servers[i].ping = data.msg or (new Date()).toTimeString()
            $scope.servers[i].pingTime = Math.round(Number(data.time) * 1000 * 1000) / 1000.0
          error: (err) -> 
            $scope.servers[i].online = false
            $scope.servers[i].leader = undefined
  
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