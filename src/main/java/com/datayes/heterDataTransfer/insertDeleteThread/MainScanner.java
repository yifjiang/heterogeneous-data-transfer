package com.datayes.heterDataTransfer.insertDeleteThread;
import com.datayes.heterDataTransfer.insertDeleteThread.Config;


import java.io.*;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lyhdk7 on 2018/6/18.
 */
public class MainScanner extends Thread{

    Connection con;
    String currentTable;

    public MainScanner(String tableName) throws ClassNotFoundException, SQLException {
        con = DriverManager.getConnection(Config.sqlConnectionUrl);
        currentTable = tableName;
    }

    Connection getConnection(){
        return con;
    }

    public void run() {
        try {
            while (true) {
                //System.out.println("hello world");

                File readFile = new File("./a.txt");
                if (!readFile.exists()) {
                    readFile.createNewFile();
                }
                List<String> getLines = readFile2(readFile);
                long newestId = 0;
                long newestTMP = 0;
                if (getLines.size() != 0) {
                    newestId = Long.parseLong(getLines.get(0));
                    newestTMP = Long.parseLong(getLines.get(1));
                }

                Statement stmt = con.createStatement();
                ResultSet rst = stmt.executeQuery(String.format("SELECT * FROM %s WHERE TMSTAMP > %d;",
                        currentTable,
                        newestTMP));

                long largestID = newestId;
                long largestTMP = newestTMP;

                while (rst.next()){
                    //long timeStamp = rst.getLong("TMSTAMP");
                    //assert timeStamp-begin <= Integer.MAX_VALUE;
                    //ret.set((int)(timeStamp-begin));
                    long curId = rst.getLong("ID");
                    long curTMP = bytesToLong(rst.getBytes("TMSTAMP"));
                    if (curId > largestID) largestID = curId;
                    if (curTMP > largestTMP) largestTMP = curTMP;
                    if (curId > newestId) {
                        System.out.println("new insert id: " + curId);
                    } else {
                        System.out.println("new update id: " + curId + " time: " + curTMP);
                    }
                }

                BufferedWriter out = new BufferedWriter(new FileWriter("./a.txt"));

                out.write(largestID + "\n");
                out.write(largestTMP + "\n");
                out.close();



                Thread.currentThread().sleep(2000);
            }
        } catch(InterruptedException ex) {
            System.out.println("Interrupt");
        } catch(IOException e) {
            System.out.println("create file fail!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    private static List<String> readFile2(File fin) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fin));
        List<String> result = new ArrayList<>();
        String line = null;
        while ((line = br.readLine()) != null) {
            result.add(line);
        }

        br.close();
        return result;
    }

    private static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }


}
