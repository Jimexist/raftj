all_servers = ("localhost:#{i}" for i in [17001, 17002, 17003, 17004, 17005])

angular.module('project', [])

.controller('ServerStatusController', () -> 

  serverStatus = this
  
  serverStatus.servers = ({address: server, status: 1, leaderID: server, ping: 1} for server in all_servers)
  
  serverStatus.restart = (address) ->
    console.log "restarting #{address}"
    $.ajax
      url: encodeURI("/api/servers/#{address}/start")
      method: 'POST'
      success: (data) -> console.log data
      error: (err) -> console.log err
  
  serverStatus.stop = (address) ->
    console.log "stopping #{address}"
    $.ajax
      url: encodeURI("/api/servers/#{address}/stop")
      method: 'POST'
      success: (data) -> console.log data
      error: (err) -> console.log err
)

.controller('ClientController', () ->

  client = this
  
)