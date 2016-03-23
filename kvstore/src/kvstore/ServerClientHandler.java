package kvstore;

import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {

    public KVServer kvServer;
    public ThreadPool threadPool;

    /**
     * Constructs a ServerClientHandler with ThreadPool of a single thread.
     *
     * @param kvServer KVServer to carry out requests
     */
    public ServerClientHandler(KVServer kvServer) {
        this(kvServer, 1);
    }

    /**
     * Constructs a ServerClientHandler with ThreadPool of thread equal to
     * the number passed in as connections.
     *
     * @param kvServer KVServer to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public ServerClientHandler(KVServer kvServer, int connections) {
        // implement me
    		this.kvServer = kvServer;
    		threadPool = new ThreadPool(connections);
    		kvServer.getDataStore().restoreFromFile(KVConstants.FILENAME);
    		writeBackDisk();
    }
    
    /**
     * Write key-value stores in the memory to disk every 15 seconds.
     */
    private void writeBackDisk(){
    		new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						while(true){
							Thread.sleep(15000);
							kvServer.getDataStore().dumpToFile(KVConstants.FILENAME);
							System.out.println("Write key-value stores in the memory to disk every 15 seconds.");
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}).start();
    }

    /**
     * Creates a job to service the request for a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        // implement me
    		threadPool.addJob(new ClientHandler(client));
    }
    
    /**
     * Runnable class containing routine to service a request from the client.
     */
    private class ClientHandler implements Runnable {

        private Socket client;

        /**
         * Construct a ClientHandler.
         *
         * @param client Socket connected to client with the request
         */
        public ClientHandler(Socket client) {
            this.client = client;
        }

        /**
         * Processes request from client and sends back a response with the
         * result. The delivery of the response is best-effort. If we are
         * unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            // implement me
        		KVMessage response = null;			
			KVMessage msg;
			
			try {
				msg = new KVMessage(client);
				switch (msg.getMsgType()) {
				case PUT_REQ:
	                response = new KVMessage(RESP, SUCCESS);
	                kvServer.put(msg.getKey(), msg.getValue());
	                break;
	            case DEL_REQ:
	                response = new KVMessage(RESP, SUCCESS);
	                kvServer.del(msg.getKey());
	                break;
	            case GET_REQ:
	                response = new KVMessage(RESP);
	                response.setValue(kvServer.get(msg.getKey()));
	                response.setKey(msg.getKey());
	                break;
	            default:
	            		break;
				}
			} catch (KVException kve) {
				response = kve.getKVMessage();
			}	
			
			try {
				response.sendMessage(client);
			} catch (KVException kve) {
				kve.printStackTrace();
			}
		}
    }
   
}
