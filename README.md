# Key-Value Store

##Overview
        Multiple clients will be communicating with a single key-value server in a given messaging format 
    (KVMessage) using a KVClient. Communication between the clients and the server will take place over the
    network through sockets (SocketServer andServerClientHandler). The KVServer uses a ThreadPool to support 
    concurrent operations across multiple sets in a set-associative KVCache, which is backed by a KVStore.
    
    ![alt tag](https://drive.google.com/file/d/0B2pTzUAE6CoHN2MxWFJXUWhlY1k/view)
