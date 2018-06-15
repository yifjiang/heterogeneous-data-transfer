package com.datayes.heterDataTransfer.sync;

public class FullSyncServer {

    ClientServerSocket serverSocket;
    ServerConfig serverConfig;

    FullSyncServer(){
        serverConfig = new ServerConfig();
        serverSocket = new ClientServerSocket(serverConfig.getHostAddress(),
                serverConfig.getHostPort());
    }

    public void start(){
        ServerSocketCreater ssc = new ServerSocketCreater(serverConfig.getHostPort());
        while (true){
            ClientServerSocket clientServerSocket = ssc.accept();
            FullSyncServerThread fullSyncServerThread
                    = new FullSyncServerThread(clientServerSocket);
            fullSyncServerThread.start();
        }
    }

    public static void main(String[] args){
        new FullSyncServer().start();
    }
}
