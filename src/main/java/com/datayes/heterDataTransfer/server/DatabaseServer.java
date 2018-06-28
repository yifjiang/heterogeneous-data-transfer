package com.datayes.heterDataTransfer.server;

import com.datayes.heterDataTransfer.insertDeleteThread.DeleteScanner;
import com.datayes.heterDataTransfer.insertDeleteThread.MainScanner;

public class DatabaseServer {

    public static void main(String[] args) {

        try {

            MainScanner insertUpdateScanner = new MainScanner("test");
            insertUpdateScanner.start();

            DeleteScanner deleteScanner = new DeleteScanner("test");
            deleteScanner.start();

        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
