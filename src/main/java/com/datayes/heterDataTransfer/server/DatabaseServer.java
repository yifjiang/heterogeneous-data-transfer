package com.datayes.heterDataTransfer.server;

import com.datayes.heterDataTransfer.scanner.DeleteScanner;
import com.datayes.heterDataTransfer.scanner.InsertUpdateScanner;

public class DatabaseServer implements Runnable {

    public void run() {
        try {

            InsertUpdateScanner insertUpdateScanner = new InsertUpdateScanner();
            insertUpdateScanner.start();

            DeleteScanner deleteScanner = new DeleteScanner();
            deleteScanner.start();

        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {

        new DatabaseServer().run();

    }
}
