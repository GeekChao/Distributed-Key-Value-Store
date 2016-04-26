package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TPCMaster {

    public int numSlaves;
    public KVCache masterCache;
    private HashMap<Long, TPCSlaveInfo> slaveMap;
    private Lock mLock;
    private Condition sync;
    private int MINVALUE = -1;
    
    public static final int TIMEOUT = 3000;

    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        // implement me
        slaveMap = new HashMap<Long, TPCSlaveInfo>();
        mLock = new ReentrantLock();
        sync = mLock.newCondition(); 
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered. Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public void registerSlave(TPCSlaveInfo slave) {
        // implement me
    		mLock.lock();
    		if(getNumRegisteredSlaves() < numSlaves)
    			slaveMap.put(slave.getSlaveID(), slave);
		//work as a barrier for sync
		if(getNumRegisteredSlaves() == numSlaves)
			sync.signalAll();
    		mLock.unlock();
    }

    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }

    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
    		long slaveId = hashTo64bit(key);
    		long repId = MINVALUE;
    		long minId = MINVALUE;
    		
    		for(long id : slaveMap.keySet()){
    			if(isLessThanEqualUnsigned(slaveId, id) && isLessThanUnsigned(id, repId))
    				repId = id;
    			
    			if(isLessThanUnsigned(id, minId))
    				minId = id;
    		}
    		
    		if(repId == MINVALUE && !slaveMap.containsKey(MINVALUE))
    			repId = minId;
    		
        return slaveMap.get(repId);
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
        long slaveId = firstReplica.slaveID;
		long repId = MINVALUE;
		long minId = MINVALUE;
		
		for(long id : slaveMap.keySet()){
			if(isLessThanUnsigned(slaveId, id) && isLessThanUnsigned(id, repId))
				repId = id;
			
			if(isLessThanUnsigned(id, minId))
				minId = id;
		}
		
		if(repId == MINVALUE || slaveId == MINVALUE)
			repId = minId;
		
		return slaveMap.get(repId);    
    }

    /**
     * @return The number of slaves currently registered.
     */
    public int getNumRegisteredSlaves() {
        // implement me
        return slaveMap.size();
    }

    /**
     * (For testing only) Attempt to get a registered slave's info by ID.
     * @return The requested TPCSlaveInfo if present, otherwise null.
     */
    public TPCSlaveInfo getSlave(long slaveId) {
        // implement me
        return slaveMap.get(slaveId);
    }

    /**
     * wait until all slaves register before servicing any requests.
     */
    	private void Barrier(){
    		try {
    			  mLock.lock();
    	    		  if(getNumRegisteredSlaves() < numSlaves)
    				sync.await();
    			} catch (InterruptedException e1) {
    				e1.printStackTrace();
    			} finally{
    				mLock.unlock();
    			}
    	}
    	
    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq)
            throws KVException {
        // implement me
    		Barrier();
    		
    		String key = msg.getKey();
    		Socket sock = null;
    		KVMessage[] response = new KVMessage[2];
    		TPCSlaveInfo[] slave = new TPCSlaveInfo[2];

    		//phase 1
    		slave[0] = findFirstReplica(key);
    		slave[1] = findSuccessor(slave[0]);
    		for(int i = 0; i < 2; i++){
			try {
				//connect two slaves
				sock = slave[i].connectHost(TIMEOUT);
				//send client requests to two slaves
				msg.sendMessage(sock);
				//get responses from slaves
				response[i] = new KVMessage(sock, TIMEOUT);
			} catch(KVException kve){
				//assume the slave voted abort beyond a single timeout period
				if(kve.getKVMessage().getMessage().equals(ERROR_SOCKET_TIMEOUT)){
					response[i] = new KVMessage(ABORT);
				}else{
					response[i] = kve.getKVMessage();
				}
			}finally{
				if(sock != null)
					slave[i].closeHost(sock);
			}
    		}
    		
    		//phase2
		KVMessage decision = null;	    		
		//master makes a decision according to the responses from slaves
    		if(response[0].getMsgType().equals(READY) && response[1].getMsgType().equals(READY)){ // two slaves are all ready
    			decision = new KVMessage(COMMIT);
    		}else{	    
    			decision = new KVMessage(ABORT);
    		}  
    		
    		for(int i = 0; i < 2; i++){
    		//during the phase 2, the master must retry with timeout until it receives a ack to its decision from slave.
    			while(true){
        			try {
        				//retry with the latest host-port the slave has registered with
        				if(i == 0){
        					slave[0] = findFirstReplica(key);
        				}else if(i == 1){
        					slave[1] = findSuccessor(slave[0]);
        				}
        				//connect two slaves
        				sock = slave[i].connectHost(TIMEOUT);
        				//send decision to two slaves
            			decision.sendMessage(sock);
            			//get response from two slaves
        				response[i] = new KVMessage(sock, TIMEOUT);
        			} catch(KVException kve){
        				continue;
        			}finally{
        				if(sock != null)
        					slave[i].closeHost(sock);
        			}

        			if(!response[i].getMsgType().equals(ACK)){
		    			throw new KVException(ERROR_INVALID_FORMAT);		    			
        			}else{
        				break;
        			}
    			}
        	}

		//update the cache in the master
		if(decision.getMsgType().equals(COMMIT)){
			Lock lock = masterCache.getLock(key);
			lock.lock();
			if(isPutReq){
				masterCache.put(key, msg.getValue());
			}else{
				masterCache.del(key);
			}
			lock.unlock();
		}
	}

    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVExceptions from both replicas
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from either slave for any reason
     */
    public String handleGet(KVMessage msg) throws KVException {
        // implement
    		Barrier();
    		
    		String key = msg.getKey();
    	    Lock lock = masterCache.getLock(key);
    		String value = null;
    		TPCSlaveInfo slaveInfo = null;
    		
    		//try to get from cache in the master server.
    		try {
			  lock.lock();
			  
			  value = masterCache.get(key);
			  
			  if(value == null){
	    			//try to get from the primary slave server.
	    			slaveInfo = findFirstReplica(key);
	    			value = getFromSlave(msg, slaveInfo);
			  }
			  
  			 if(value == null){
  	  			//if it fails, try to get from the secondary slave server.
  				slaveInfo = findSuccessor(slaveInfo);
  				value = getFromSlave(msg, slaveInfo);
  			 }
  			 
  			 if(value != null)
  				 masterCache.put(key, value);
  			
			} catch (Exception e) {
			} finally{
				lock.unlock();
			}
    		
    		value = "bar";
    				
    		//all fail
    		if(value == null)
    			throw new KVException(ERROR_NO_SUCH_KEY);
    		
        return value;
    }
    
    private String getFromSlave(KVMessage msg, TPCSlaveInfo slaveInfo){
    		Socket sock = null;
    		KVMessage response = null;
    		String value = null;
    		
    		try {
    				//transfer get requst from clients to slave server
				sock = slaveInfo.connectHost(TIMEOUT);
				msg.sendMessage(sock);
				
				//get responese from slaves
				response = new KVMessage(sock, TIMEOUT);
				value = response.getValue();
				
			} catch (KVException e) {
				e.printStackTrace();
			} finally{
				if(sock != null)
					slaveInfo.closeHost(sock);
			}
    		
    		return value;
    }

}
