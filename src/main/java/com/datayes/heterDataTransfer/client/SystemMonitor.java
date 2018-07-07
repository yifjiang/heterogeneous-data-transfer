package com.datayes.heterDataTransfer.client;

import com.jezhumble.javasysmon.JavaSysMon;
import com.jezhumble.javasysmon.MemoryStats;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SystemMonitor extends Thread{

    Connection con;

    SystemMonitor(Connection inCon){
        con = inCon;
    }

    @Override
    public void run() {
        super.run();
        try {
            Statement stmt =  con.createStatement();
            String createIfNotExists = "CREATE TABLE IF NOT EXISTS sysRecord(recordID BIGINT AUTO_INCREMENT PRIMARY KEY, cpuFrequency BIGINT, freeMemory BIGINT, dateAndTime DATETIME DEFAULT NOW())";
            stmt.execute(createIfNotExists);
            JavaSysMon javaSysMon = new JavaSysMon();
            while (true){
                long cpuFreqeuncy = javaSysMon.cpuFrequencyInHz();
                MemoryStats memoryStats = javaSysMon.swap();
                String recordQuery = "INSERT INTO sysRecord(cpuFrequency, freeMemory) VALUES ("+Long.toString(cpuFreqeuncy)+", "+Long.toString(memoryStats.getFreeBytes())+")";
                System.out.println(memoryStats);
                stmt.execute(recordQuery);
                sleep(1000);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
