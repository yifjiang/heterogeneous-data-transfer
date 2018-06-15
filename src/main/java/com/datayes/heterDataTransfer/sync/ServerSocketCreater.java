package com.datayes.heterDataTransfer.sync;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static java.lang.System.out;

public class ServerSocketCreater {
    ServerSocket serverSock;
    int portNum;
//    ClientServerSocket [] sockets;

    ServerSocketCreater(int _portNum){
        portNum = _portNum;
        try {
            serverSock = new ServerSocket(portNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ClientServerSocket accept() {
        try {
            out.println("Waiting for clients' request, at port=" + portNum);

            Socket socket = serverSock.accept();

            out.println("Accepted client request");

            ClientServerSocket ret = new ClientServerSocket(socket);

            return ret;
        } catch (IOException ioe) {
            out.println("Accepting client connection failed");
            ioe.printStackTrace();
//            System.exit(7);
        }
        return null;
    }

    public void close(){
        try {
            serverSock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
