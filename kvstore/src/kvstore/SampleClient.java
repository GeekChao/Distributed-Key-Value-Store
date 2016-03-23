package kvstore;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;

public class SampleClient {
	
	private static String hostname;
	private static Thread[] clientThread = new Thread[3];
	private static final int PUT = 0;
	private static final int GET = 1;
	private static final int DEL = 2;
	
    /**
     * A client is responsible for retrieving key-value store
     */
	private static void clientGet(){
		try{
			KVClient client = new KVClient(hostname, 9999);

			Thread curThread = Thread.currentThread();        
	        System.out.println(curThread.getName() + "get(\"foo\")");
	        String value = client.get("foo");
	        System.out.println(curThread.getName() + "Get returned \"" + value + "\"");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(3000));
	        
	        System.out.println(curThread.getName() + "get(\"hello\")");
	        value = client.get("hello");
	        System.out.println(curThread.getName() + "Get returned \"" + value + "\"");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(3000));
	        
	        System.out.println(curThread.getName() + "get(\"how are u\")");
	        value = client.get("how are u");
	        System.out.println(curThread.getName() + "Get returned \"" + value + "\"");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(3000));

		}
        catch (KVException kve) {
            System.out.println("ERROR: Unexpected KVException raised: ");
            System.out.println("Message: " + kve.getKVMessage().getMessage());
            kve.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
    /**
     * A client is responsible for writing key-value store
     */
	private static void clientPut(){
		try{
			KVClient client = new KVClient(hostname, 9999);

			Thread curThread = Thread.currentThread();
	        System.out.println(curThread.getName() + "put(\"foo\", \"bar\")");
	        client.put("foo", "bar");
	        System.out.println(curThread.getName() + "put success!");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(2000));
	        
	        System.out.println(curThread.getName() + "put(\"hello\", \"world\")");
	        client.put("hello", "world");
	        System.out.println(curThread.getName() + "put success!");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(3000));
	        
	        System.out.println(curThread.getName() + "put(\"how are u\", \"fine\")");
	        client.put("how are u", "fine");
	        System.out.println(curThread.getName() + "put success!");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(2000));

		}
        catch (KVException kve) {
            System.out.println("ERROR: Unexpected KVException raised: ");
            System.out.println("Message: " + kve.getKVMessage().getMessage());
            kve.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

    /**
     * A client is responsible for deleting key-value store
     */
	private static void clientDel(){
		try{
			KVClient client = new KVClient(hostname, 9999);

			Thread curThread = Thread.currentThread();
	        System.out.println(curThread.getName() + "del(\"foo\")");
	        client.del("foo");
	        System.out.println(curThread.getName() + "del success!");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(10000));
	        
	        System.out.println(curThread.getName() + "del(\"hello\")");
	        client.del("hello");
	        System.out.println(curThread.getName() + "del success!");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(10000));
		}
        catch (KVException kve) {
            System.out.println("ERROR: Unexpected KVException raised: ");
            System.out.println("Message: " + kve.getKVMessage().getMessage());
            kve.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
    /**
     * Create a thread for each client according to its type
     * @param  Three type of client: put, get and delete
     */
	private static Thread createClientFactory(int type){
		Thread temp = null;
		switch (type) {
		case PUT:
			temp = new Thread(new Runnable() {
				
				@Override
				public void run() {
					while(true){
						clientPut();
					}
				}
			});
			return temp;
		case GET:
			temp = new Thread(new Runnable() {
				
				@Override
				public void run() {
					while(true){
						clientGet();
					}
				}
			});
			return temp;
			case DEL:
				temp = new Thread(new Runnable() {
					
					@Override
					public void run() {
						while(true){
							clientDel();
						}
					}
				});
				return temp;
			default:
			break;
		}
		return temp;
	}
	
    public static void main(String[] args){

        if (args.length != 1) {
            throw new IllegalArgumentException("Need server IP address");
        }

        hostname = args[0];

        try {
            if (hostname.charAt(0) == '$') {
            		hostname = InetAddress.getLocalHost().getHostAddress();
            }
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
        
        System.out.println("Looking for server at " + hostname);   
        
        //create three client threads
        for(int i = 0; i < 3; i++){
        		clientThread[i] = createClientFactory(i);
        		clientThread[i].setName("client " + i + ": ");
        		clientThread[i].start();
        }  
    }
}
