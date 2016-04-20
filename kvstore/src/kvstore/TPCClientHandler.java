package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * It uses a threadPool to ensure that none of it's methods are blocking.
 */
public class TPCClientHandler implements NetworkHandler {

    public TPCMaster tpcMaster;
    public ThreadPool threadPool;

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     */
    public TPCClientHandler(TPCMaster tpcMaster) {
        this(tpcMaster, 1);
    }

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCClientHandler(TPCMaster tpcMaster, int connections) {
        // implement me
    		this.tpcMaster = tpcMaster;
    		threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        // implement me
    		threadPool.addJob(new ClientHandler(client));
    }
    
    // implement me
    private class ClientHandler implements Runnable{

    		private Socket client;
    		
    		public ClientHandler(Socket client){
    			this.client = client;
    		}
    		
		@Override
		public void run() {
    			KVMessage response = null;			
    			KVMessage msg;
    			
    			try {
					msg = new KVMessage(client);
					switch (msg.getMsgType()) {
					case GET_REQ:
						String value = tpcMaster.handleGet(msg);
						response = new KVMessage(RESP);
						response.setKey(msg.getKey());
						response.setValue(value);
						break;
					case PUT_REQ:
						tpcMaster.handleTPCRequest(msg, true);
						response = new KVMessage(RESP, SUCCESS);
						break;
					case DEL_REQ:
						tpcMaster.handleTPCRequest(msg, false);
						response = new KVMessage(RESP, SUCCESS);					
					default:
						break;
					}
				} catch (KVException kve) {
					response = kve.getKVMessage();
				}
    			
				try {	    					
	    				response.sendMessage(client);
				} catch (KVException e) {

				}
		}
    	
    }

}
