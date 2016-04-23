package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.*;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {

    public long slaveID;
    public String hostname;
    public int port;

    /**
     * Construct a TPCSlaveInfo to represent a slave server.
     *
     * @param info as "SlaveServerID@Hostname:Port"
     * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
     */
    public TPCSlaveInfo(String info) throws KVException {
        // implement me
    		if(info == null || !info.contains("@") || !info.contains(":"))
    			throw new KVException(ERROR_INVALID_FORMAT);
    			
    		int index1 = info.indexOf("@");
    		int index2 = info.indexOf(":");
    		
    		slaveID = Long.parseLong(info.substring(0, index1));
    		hostname = info.substring(index1 + 1, index2);
    		port = Integer.parseInt(info.substring(index2 + 1));
    }

    public long getSlaveID() {
        return slaveID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Create and connect a socket within a certain timeout.
     *
     * @return Socket object connected to SlaveServer, with timeout set
     * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
     *         or ERROR_COULD_NOT_CONNECT
     */
    public Socket connectHost(int timeout) throws KVException {
        // implement me
    		Socket sock = null;
    		
    		try {
				sock = new Socket(hostname, port);
				sock.setSoTimeout(timeout);
			} catch(SocketTimeoutException e){
				throw new KVException(ERROR_SOCKET_TIMEOUT);
			}catch (UnknownHostException e) {
				throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
			} catch (IOException e) {
				throw new KVException(ERROR_COULD_NOT_CONNECT);
			} 
    		
        return sock;
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param sock Socket to be closed
     */
    public void closeHost(Socket sock) {
        // implement me
    		try {
				sock.close();
			} catch (IOException e) {
			}
    }
}
