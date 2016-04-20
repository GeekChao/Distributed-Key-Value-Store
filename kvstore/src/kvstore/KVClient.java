package kvstore;

import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.ERROR_COULD_NOT_CONNECT;
import static kvstore.KVConstants.ERROR_COULD_NOT_CREATE_SOCKET;
import static kvstore.KVConstants.ERROR_INVALID_KEY;
import static kvstore.KVConstants.ERROR_INVALID_VALUE;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

//import com.apple.eawt.event.MagnificationEvent;

/**
 * Client API used to issue requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

    public String server;
    public int port;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port on which the server is listening
     */
    public KVClient(String server, int port) {
        this.server = server;
        this.port = port;
    }

    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to create or connect socket
     */
    public Socket connectHost() throws KVException {
        // implement me
		try {
			return new Socket(server, port);
		} catch (UnknownHostException e) {
			throw new KVException(ERROR_COULD_NOT_CONNECT);
		} catch (IOException e) {
			throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
		}
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param  sock Socket to be closed
     */
    public void closeHost(Socket sock) {
        // implement me
    		try {
				sock.close();
			} catch (IOException e) {
			}
    }

    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
        // implement me
    		Socket sock = null;
    		try {
    				if (null == key || key.isEmpty()) throw new KVException(ERROR_INVALID_KEY);
    				if (null == value || value.isEmpty()) throw new KVException(ERROR_INVALID_VALUE);
    				
    				sock = connectHost();
    				//send 
    				KVMessage outMsg = new KVMessage(PUT_REQ);
    				outMsg.setKey(key);
    				outMsg.setValue(value);
    				outMsg.sendMessage(sock);
    				//receive
    				KVMessage inMsg = new KVMessage(sock);
    				
    				if(!inMsg.getMessage().equals(SUCCESS)) throw new KVException(inMsg.getMessage());
    				
			} catch (KVException kve) {
				throw kve;
			} finally{
				if(sock != null)
					closeHost(sock);
			}
    }

    /**
     * Issues a GET request to the server.
     *
     * @param  key String to get value for in server
     * @return String value associated with key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public String get(String key) throws KVException {
        // implement me
    		Socket sock = null;
    		KVMessage inMsg;
		try {
				if (null == key || key.isEmpty()) throw new KVException(ERROR_INVALID_KEY);
				
				sock = connectHost();
				//send 
				KVMessage outMsg = new KVMessage(GET_REQ);
				outMsg.setKey(key);
				outMsg.sendMessage(sock);
				//receive
				inMsg = new KVMessage(sock);
				
				if(null == inMsg.getKey() || null == inMsg.getValue())
					throw new KVException(inMsg.getMessage());
								
		} catch (KVException kve) {
			throw kve;
		} finally{
			if(sock != null)
				closeHost(sock);
		}
		
		return inMsg.getValue();

    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
        // implement me
    		Socket sock = null;
		try {
				if (null == key || key.isEmpty()) throw new KVException(ERROR_INVALID_KEY);
				
				sock = connectHost();
				//send 
				KVMessage outMsg = new KVMessage(DEL_REQ);
				outMsg.setKey(key);
				outMsg.sendMessage(sock);
				//receive
				KVMessage inMsg = new KVMessage(sock);
				
				if(!inMsg.getMessage().equals(SUCCESS)) throw new KVException(inMsg.getMessage());
				
		} catch (KVException kve) {
			throw kve;
		} finally{
			if(sock != null)
				closeHost(sock);
		}
    }


}
