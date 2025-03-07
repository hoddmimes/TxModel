
## Quorum, Service States

### Case 1, Cold Start
* The quorum server is started, will start with primary and stanby state to UNKNOWN.


* Will wait for 2 x HB interval, before serving any vote request to see whatever any server is active and already is operating in a state


* If a first vote request is received the quorum server will holdback 'n' seconds before making a decision. This will allow the server to receive another vote request for the other primary/standby server.