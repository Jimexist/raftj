angular.module('project', [])

.controller('ServerStatusController', ['$scope', '$interval', ($scope, $interval) -> 

  $scope.servers = []
  
  $scope.sending = false
  
  $scope.init = () ->
    $.ajaxSetup
        timeout: 2000
    
    $.get "/api/servers", (data) ->
      addresses = JSON.parse(data)
      addresses.sort()
      $scope.servers = ({
        address: address
      } for address in addresses)
      $scope.$apply()
  
  $scope.restart = (address) ->
    $.ajax
      url: encodeURI("/api/servers/#{address}/restart")
      method: 'POST'
      success: (data) -> 
        alert data
      error: (xhr, ajaxOptions, err) -> 
        alert err
  
  $scope.start = (address) ->
    $.ajax
      url: encodeURI("/api/servers/#{address}/start")
      method: 'POST'
      success: (data) -> 
        alert data
      error: (xhr, ajaxOptions, err) -> 
        alert err
  
  $scope.stop = (address) ->
    $.ajax
      url: encodeURI("/api/servers/#{address}/stop")
      method: 'POST'
      success: (data) -> 
        alert data
      error: (xhr, ajaxOptions, err) -> 
        alert err
        
  $scope.send_client_command = (address, command) ->
    $.ajax
      url: encodeURI("/api/servers/#{address}/command")
      method: 'POST'
      contentType: 'application/json'
      data: JSON.stringify({'command': command})
      success: (data) ->
        resp = JSON.parse(data)
        alert "successfully sent command, reply message: '#{resp.message}', request time: #{resp.time}s"
      error: (xhr, ajaxOptions, err) ->
        alert err
  
  $scope.updateStatus = () ->
    for i in [0..($scope.servers.length-1)]
      do (i) ->
        address = $scope.servers[i].address
        $.ajax
          url: encodeURI("/api/servers/#{address}/status")
          success: (data) ->
            data = JSON.parse(data)
            now = new Date()
            $scope.servers[i].online = true
            $scope.servers[i].leader = data.status
            $scope.servers[i].leaderID = data.leaderID
            $scope.servers[i].ping = data.msg or "#{now.toISOString()}"
            $scope.servers[i].pingTime = Math.round(Number(data.time) * 1000 * 1000) / 1000.0
          error: (xhr, ajaxOptions, err) -> 
            $scope.servers[i].online = false
            $scope.servers[i].leader = undefined
            $scope.servers[i].leaderID = undefined
  
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