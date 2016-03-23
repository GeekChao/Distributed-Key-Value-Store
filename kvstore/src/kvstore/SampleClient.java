package kvstore;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;

public class SampleClient {
	
	private static String hostname;
	private static Thread[] clientThread = new Thread[3];
	
	private static void client(){
		try{
			KVClient client = new KVClient(hostname, 9999);

			Thread curThread = Thread.currentThread();
	        System.out.println(curThread.getName() + "put(\"foo\", \"bar\")");
	        client.put("foo", "bar");
	        System.out.println(curThread.getName() + "put success!");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(1000));
	        
	        System.out.println(curThread.getName() + "get(\"foo\")");
	        String value = client.get("foo");
	        System.out.println(curThread.getName() + "Get returned \"" + value + "\"");
	        Thread.sleep(ThreadLocalRandom.current().nextInt(10000));

	        System.out.println(curThread.getName() + "del(\"foo\")");
	        client.del("foo");
	        System.out.println(curThread.getName() + "del success!");

		}
        catch (KVException kve) {
            System.out.println("ERROR: Unexpected KVException raised: ");
            System.out.println("Message: " + kve.getKVMessage().getMessage());
            kve.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        		clientThread[i] = new Thread(new Runnable() {
					
					@Override
					public void run() {
						while(true){
							client();
						}
					}
				});
        		clientThread[i].setName("client " + i + ": ");
        		clientThread[i].start();
        }  
    }
}
