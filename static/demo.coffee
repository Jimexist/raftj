all_servers = ("localhost:#{i}" for i in [17001, 17002, 17003, 17004, 17005])

angular.module('project', [])

.controller('ServerStatusController', () -> 

  serverStatus = this
  
  serverStatus.servers = ({address: server, status: 1, leaderID: server, ping: 1} for server in all_servers)
  
  serverStatus.stop = (address) ->
    console.log "stopping #{address}"
    $.ajax
      url: encodeURI("/api/stop/#{address}")
      method: 'POST'
      success: () -> console.log "success"
      error: () -> console.log "error"
)

.controller('ClientController', () ->

  client = this
  
)