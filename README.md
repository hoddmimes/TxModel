# TxTest

The _TxTest_ project is a conceptual initiative aimed at exploring how solution redundancy—specifically, 
hot standbys—can be implemented in high-performance, low-latency systems.

By hot standby, we refer to an application instance running in parallel with the primary system, 
ready to take over in the event of a primary failure. The transition from primary to standby should occur 
with minimal delay—ideally within a fraction of a second—after detecting a failure in the primary system.

High-performance systems are those capable of processing thousands possible millions of complex events per second. 
Examples include exchange systems that handle order processing, where an order triggers the matching 
of orders in an order book, updates the state of existing orders, and potentially executes trades. 
Such systems typically operate on in-memory data structures to achieve the necessary speed and efficiency.
The latency of such system, end-to-end are typical in micro seconds rather than milliseconds.

## About the TxTest, Transaction Model

A TxTest server maintains a set of assets that can be updated by clients. 
Clients submit update events to modify these assets. Each client operates as an autonomous entity, executing in parallel.

The server ensures that each asset update is processed atomically, guaranteeing consistency. 
However, updates to different assets may occur in parallel.

Each client update request is resulting in a response sent from the server to the client, returning the outcome of the update.



## Redundancy Model

In a brief overview, multiple clients submits update events of _assets_ to a (primary) server for processing. The server is handling 
multiple assets. The server guarantees that processing for an asset is done automatically and only once.

Multiple updates for the same asset may arrive more or less simultanously. Updates will be queued for singel processing. 
However processing of updates for different assets may occur in parallel.

The processing steps are;

1. Receive asynchronously an update request from a client.
2. Queue the request for processing. Depening on asset a dedicated thread for the specified asset is selected. This guarante atomic processing.
3. When executing the processing step, the following steps occurs;
    - Asynchronsly queue the update request to the standby server.
    - Asynchronsly queue the update request to the transaction log
    - Perform update of the asset data model based upon the request
    - wait for the stanby to confirm that it has received the update request
    - sending confirmation to the client about the processing result of the update
    - possible publishing general updates as of the update.


## Failover

The standby server will have a connection to the primmary. Hearbeat messages are constantly exchanged between primary and standby.
When a configurable number of heartbeat has been missed the counterparty node is considered to be lost and a possible failover 
may take place.




## Restart and Recovery

When a node is started it will recover to the last known state. This is accomplished by replaying the transaction logfiles.




When having a primary standby pair it needs to be determined who should be the primary and standby. If no history exists 
and th host are equal any node can be primary. Traditional solutions for such setups are third node solution acting as a quorum server
and using protocols some consensus protocol such as _Consul_ or _Raft_.

When having two autonomus nodes, having a history dependency the situtation is a bit more complex in case of a total restart.
Assume N1 and N2 where N1 was selected as primary and N2 as standby. We will have a number of restart scenarios which are described below.

* N1 runs and N2 failes. N1 continues as primary and N2 if restarted will synch up with N1 and becomme standby.
* N2 runs as standby and N1 failes. N2 will take over and continue as primary. If N1 is restarted it will synch up with N2 and become standby.
* _**N1 runs and primary and N2 as standby. Both nodes failes or stops. In case of a restart it important that the last node being primary is started as primary.**_

For the scenario where both nodes are restarted there is a history. The primary is equal with the standby but likely ahead of the standby.
_The startup under such condition it's essential that the latest primary node also is started as primary._ 



