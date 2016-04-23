package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import kvstore.xml.KVMessageType;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    public long slaveID;
    public KVServer kvServer;
    public TPCLog tpcLog;
    public ThreadPool threadpool;

    // implement me
    private static final int MAX_KEY_SIZE = 256;
    private static final int MAX_VAL_SIZE = 256 * 1024;
    
    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
        // implement me
    		String slaveInfo = slaveID + "@" + server.getHostname() + ":" + server.getPort();
    		KVMessage msg = new KVMessage(REGISTER, slaveInfo);
    		Socket sock = null;
    		
    		try {
    			//send slave information to master
				sock = new Socket(masterHostname, 9090);
				msg.sendMessage(sock);
			//receive response from the master
				KVMessage response = new KVMessage(sock);
				if(!response.getMsgType().equals(RESP) || !response.getMessage().equals("Successfully registered " + slaveInfo))
					throw new KVException(ERROR_INVALID_FORMAT);
				
			} catch (UnknownHostException e) {
				throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
			} catch (IOException e) {
				throw new KVException(ERROR_COULD_NOT_CONNECT);
			}
    		
			try {
	    			if(null != sock)
					sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
    }
    
    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        // implement me
    		threadpool.addJob(new MasterHandler(master));
    }
    
    private class MasterHandler implements Runnable{

    		private Socket master;
    		
    		public MasterHandler(Socket master){
    			this.master = master;
    		}
    		
		@Override
		public void run() {
    			KVMessage response = null;			
    			KVMessage msg = null;
    			
    			try {
    					msg = new KVMessage(master);
    			   		String key = msg.getKey();
    					String value = msg.getValue();
    					
					switch (msg.getMsgType()) {
					case GET_REQ:
						if(key == null){
							response = new KVMessage(RESP, ERROR_INVALID_KEY);
						}else if(key.length() > MAX_KEY_SIZE){
							response = new KVMessage(RESP, ERROR_OVERSIZED_KEY);
						}else if(!kvServer.hasKey(key)){
							response = new KVMessage(RESP, ERROR_NO_SUCH_KEY);
						}else{
			                response = new KVMessage(RESP);
			                response.setValue(kvServer.get(msg.getKey()));
			                response.setKey(msg.getKey());
						}
						break;
					case PUT_REQ:
						if(key == null){
							response = new KVMessage(ABORT, ERROR_INVALID_KEY);
						}else if(key.length() > MAX_KEY_SIZE){
							response = new KVMessage(ABORT, ERROR_OVERSIZED_KEY);
						}else if(!kvServer.hasKey(key)){
							response = new KVMessage(ABORT, ERROR_NO_SUCH_KEY);
						}else if (value == null) {
							response = new KVMessage(ABORT , ERROR_INVALID_VALUE);
						}else if (value.length() > MAX_VAL_SIZE) {
							response = new KVMessage(ABORT, ERROR_OVERSIZED_VALUE);
						}else{
							response = new KVMessage(READY);
						}
						break;
					case DEL_REQ:
						if(key == null){
							response = new KVMessage(ABORT, ERROR_INVALID_KEY);
						}else if(key.length() > MAX_KEY_SIZE){
							response = new KVMessage(ABORT, ERROR_OVERSIZED_KEY);
						}else if(!kvServer.hasKey(key)){
							response = new KVMessage(ABORT, ERROR_NO_SUCH_KEY);
						}else{
							response = new KVMessage(READY);
						}
						break;
					case COMMIT:
						response = new KVMessage(ACK);
						KVMessage last = tpcLog.getLastEntry();
						if(last.getMsgType().equals(PUT_REQ)){
							kvServer.put(last.getKey(), last.getValue());
						}else if(last.getMsgType().equals(DEL_REQ)){
							kvServer.del(last.getKey());
						}
						break;
					case ABORT:
						response = new KVMessage(ACK);
						break;
					default:
						break;
					}					
					
				} catch (KVException kve) {
					response = kve.getKVMessage();
				}
    			    	
    			tpcLog.appendAndFlush(msg);

    			try {
					response.sendMessage(master);
				} catch (KVException e) {
				}
		}
    	
    }
}
