# Distributed Key-Value Store

## Overview
        Multiple clients will be communicating with a single master server in a given messaging format 
    (KVMessage) using a client library (KVClient). The master contains a set-associative cache (KVCache),
    and it uses the cache to serve GET requests without going to the key-value (slave) servers it coordinates.
    The slave servers are contacted for a GET only upon a cache miss on the master. The master will use 
    the TPCMaster library to forward client requests for PUT and DEL to multiple slave servers and follow 
    the TPC protocol for atomic PUT and DEL operations across multiple slave servers.
    
![alt tag](https://github.com/GeekChao/Operating-System/blob/distributed/kvstore/resources/pictures/kvstore.png)
