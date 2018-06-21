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

                //Execute the query
                Statement stmt = con.createStatement();
                stmt.setFetchSize(fetchSize);
                ResultSet rst = stmt.executeQuery("SELECT ID FROM " + currentTable);
                StringBuilder content = new StringBuilder();

                //Read by partition
                int start = 0;
                List<Long> preIdSet = new ArrayList<>();
                List<Long> curIdSet = new ArrayList<>();
                int prePtr = 0;
                int curPtr = 0;
                while (true){
                    if (prePtr >= preIdSet.size()){
                        preIdSet = readIdListByPartition(readFile, start, fetchSize);
                        start += fetchSize;
                        prePtr = 0;
                    }
                    if (curPtr >= curIdSet.size()){
                        curIdSet = readDataBaseByPartition(rst, fetchSize);
                        content = fileWriter(content, curIdSet);
                        curPtr = 0;
                    }
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
                    if (curIdSet.size() == 0 || preIdSet.size() == 0){
                        while (prePtr < preIdSet.size()){
                            System.out.println("new delete id: " + preIdSet.get(prePtr));
                            prePtr += 1;
                        }
                        while(curIdSet.size() > 0){
                            curIdSet = readDataBaseByPartition(rst, fetchSize);
                            content = fileWriter(content, curIdSet);
                        }
                        break;
                    }
                }
                BufferedWriter out = new BufferedWriter(new FileWriter("./backup.txt", false));

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

    private static List<Long> readIdListByPartition(File fin, int start, int size) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fin));
        List<Long> result = new ArrayList<>();
        while (start-- > 0){
            br.readLine();
        }
        String line;
        while ((line = br.readLine()) != null && size-- > 0) {
            result.add(Long.parseLong(line));
        }
        br.close();
        return result;
    }

    private static List<Long> readDataBaseByPartition(ResultSet rst, int size) throws SQLException {
        List<Long> result = new ArrayList<>();
        for (int i = 0; i < size; i += 1){
            if(rst.next()){
                result.add(rst.getLong("ID"));
            }
        }
        return result;
    }

    private static StringBuilder fileWriter(StringBuilder content, List<Long> curIdSet){
        for (int i = 0; i < curIdSet.size(); i += 1){
            content.append(String.valueOf(curIdSet.get(i)) + "\n");
        }
        return content;
    }




}
