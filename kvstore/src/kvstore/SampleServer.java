package kvstore;

import java.net.InetAddress;

import bsh.This;

import com.sun.corba.se.impl.orbutil.closure.Constant;

public class SampleServer {
	
    public static void main(String[] args) {
        try {
            String hostname = InetAddress.getLocalHost().getHostAddress();
            SocketServer ss = new SocketServer(hostname, 9999);
            ss.addHandler(new ServerClientHandler(new KVServer(100, 3)));
            ss.connect();
            System.out.println("Server listening for clients at " + ss.getHostname());
            ss.start();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
