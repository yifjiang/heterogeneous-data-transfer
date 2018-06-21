package com.datayes.heterDataTransfer.insertDeleteThread;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DeleteScanner extends Thread{

    Connection con;
    String currentTable;
    final int fetchSize = 2000;

    public DeleteScanner(String tableName) throws ClassNotFoundException, SQLException {
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

                File readFile = new File("./backup.txt");
                if (!readFile.exists()) {
                    readFile.createNewFile();
                }

                List<Long> preIdSet = readIdList(readFile);
                List<Long> curIdSet = new ArrayList<>();

                Statement stmt = con.createStatement();
                stmt.setFetchSize(fetchSize);
                ResultSet rst = stmt.executeQuery("SELECT ID FROM " + currentTable);

                while(rst.next()){
                    curIdSet.add(rst.getLong("ID"));
                }

                int prePtr = 0;
                int curPtr = 0;

                while (prePtr < preIdSet.size() && curPtr < curIdSet.size()){
                    long curId = curIdSet.get(curPtr);
                    long preId = preIdSet.get(prePtr);
                    if (curId > preId){
                        System.out.println("new delete id: " + preId);
                        prePtr += 1;
                    } else if (curId == preId){
                        prePtr += 1;
                        curPtr += 1;
                    } else{
                        curPtr += 1;
                    }
                }
                while (prePtr < preIdSet.size()){
                    long preId = preIdSet.get(prePtr);
                    System.out.println("new delete id: " + preId);
                    prePtr += 1;
                }

                BufferedWriter out = new BufferedWriter(new FileWriter("./backup.txt", false));
                StringBuilder content = new StringBuilder();
                //System.out.println("write:");
                //System.out.println(curIdSet);
                for (int i = 0; i < curIdSet.size(); i += 1){
                    content.append(String.valueOf(curIdSet.get(i)) + "\n");
                }
                out.write(content.toString());
                out.close();

                Thread.currentThread().sleep(5000);
            }
        } catch(InterruptedException ex) {
            System.out.println("Interrupt");
        } catch(IOException e) {
            System.out.println("create file fail!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    private static List<Long> readIdList(File fin) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fin));
        List<Long> result = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            result.add(Long.parseLong(line));
        }
        br.close();
        return result;
    }


}
