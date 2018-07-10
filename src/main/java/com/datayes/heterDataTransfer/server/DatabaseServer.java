package com.datayes.heterDataTransfer.server;

import com.datayes.heterDataTransfer.scanner.DeleteScanner;
import com.datayes.heterDataTransfer.scanner.InsertUpdateScanner;
import com.datayes.heterDataTransfer.scanner.MainScanner;

public class DatabaseServer implements Runnable {

    public void run() {
        try {
            MainScanner myScanner = new MainScanner();
            myScanner.start();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {

        new DatabaseServer().run();

    }
}
