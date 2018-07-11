package com.datayes.heterDataTransfer.scanner;

import com.datayes.heterDataTransfer.server.ServerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.io.*;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogScanner extends Thread{
    Connection con;
    String currentTable;
    String currentDb;
    String file_helper;

    private final Producer<String, String> producer=new KafkaProducer<>(ServerConfig.kafkaProps);

    public LogScanner() throws ClassNotFoundException, SQLException{
        con=DriverManager.getConnection(ServerConfig.sqlConnectionUrl);
        currentTable=ServerConfig.tableName;
        //currentTable="test_h7";
        file_helper=String.format("./%s_Log_helper.txt", currentTable);
        currentDb="testMSSQL";



        //turn on cdc if it's off
        //notice, if the same-name cdc capture_instance already exists, then the query will throw exception
        try{
            int i=0;

            Statement stmt = con.createStatement();
            ResultSet rst=stmt.executeQuery("SELECT [name], is_tracked_by_cdc FROM sys.tables;");

            //System.out.println(i++);

            while (rst.next()){
                String tableName=rst.getString("name");

                if(tableName.equals(currentTable)){
                    Boolean isTrackedByCdc=rst.getBoolean("is_tracked_by_cdc");

                    if(isTrackedByCdc==false){
                        stmt.execute(String.format("EXEC sys.sp_cdc_enable_table @source_schema=dbo, " +
                                "@source_name=%s, @role_name=NULL, @capture_instance=cdcAuditing_%s;",
                                currentTable, currentTable));
                        //System.out.println(i++);

                        break;
                    }
                }
            }
        }

        catch(SQLException e){
            System.out.println("Constructor");
            System.out.println("SQL exception");
            System.out.println(e.getMessage());
        }

    }

    Connection getConnection(){
        return con;
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

    private String getMinCdcLsn(){

        try{
            Statement stmt = con.createStatement();

            ResultSet rst=stmt.executeQuery("DECLARE @begin binary(10);"+
                    String.format("SET @begin=sys.fn_cdc_get_min_lsn('cdcAuditing_%s');", currentTable)+
                    "SELECT @begin;"
            );

            rst.next();
            return rst.getString(1);

        }

        catch(SQLException e){
            System.out.println("SQL exception");
            System.out.println(e.getMessage());
            return null;
        }
    }


    public void run(){
        try{
            Statement stmt = con.createStatement();

            System.out.println("run!");


            while(true){

                int p=0;

                //read the maxLsn, maxSeqval from the file
                String prevMaxLsn="";
                String prevMaxSeqval="";


                File readFile = new File(file_helper);
                if (!readFile.exists()) {
                    readFile.createNewFile();
                }
                List<String> getLines = readFile2(readFile);

                if (getLines.size() != 0) {
                    prevMaxLsn=getLines.get(0);
                    prevMaxSeqval=getLines.get(1);
                }


                //if the prevMaxLsn doesn't exist before, we have to query for it through sys.fn_cdc_get_min_lsn('cdcAuditing_test');
                //notice, the query result might be null, we need to continue querying for it
                while(prevMaxLsn==null || prevMaxLsn.isEmpty()){
                    prevMaxLsn=getMinCdcLsn();

                }

                //query to get cdc
                ResultSet rst=stmt.executeQuery("DECLARE @begin binary(10), @end binary(10);" +
                        String.format("SET @begin=0x%s; SET @end=sys.fn_cdc_get_max_lsn();", prevMaxLsn)+
                        String.format(" SELECT * FROM cdc.fn_cdc_get_all_changes_cdcAuditing_%s(@begin, @end, N'ALL');", currentTable));

                //System.out.println(p++);

                ResultSetMetaData metaData=rst.getMetaData();

                int numCol=metaData.getColumnCount();

                ArrayList<String> columnNames = new ArrayList<>(numCol);
                ArrayList<Integer> columnTypes = new ArrayList<>(numCol);
                for (int j = 1; j <= numCol; ++j){
                    columnNames.add(metaData.getColumnName(j));
                    columnTypes.add(metaData.getColumnType(j));
                }

                while(rst.next()){
                    String curLsn=rst.getString("__$start_lsn");
                    String curSeqval=rst.getString("__$seqval");


                    if(curLsn.compareTo(prevMaxLsn)>0 || (curLsn.compareTo(prevMaxLsn)==0 && curSeqval.compareTo(prevMaxSeqval)>0)){
                        //update maxLsn, maxSeqval
                        prevMaxLsn=curLsn;
                        prevMaxSeqval=curSeqval;

                        System.out.println(curLsn);
                        System.out.println(curSeqval);

                        //transform the log to mysql command, send it to kafka
                        int operationType=rst.getInt("__$operation");
                        Map<String, String> tempMap = new HashMap<>();

                        //operationType==3 means preupdate, do nothing
                        switch (operationType){
                            case 1:
                                tempMap.put("OPERATION", "DELETE");
                                break;

                            case 2:
                                tempMap.put("OPERATION", "INSERT");
                                break;

                            case 3:
                                continue;

                            case 4:
                                tempMap.put("OPERATION", "UPDATE");
                        }

                        System.out.println(tempMap.get("OPERATION"));

                        //the previous four columns are meta data
                        for (int j = 5; j <= numCol; ++j) {
                            byte[] toProcess = rst.getBytes(j);

                            tempMap.put(columnNames.get(j-1), helpToString(columnTypes.get(j-1), toProcess));
                        }

                        producer.send(new ProducerRecord<String, String>(ServerConfig.topicName,
                                null, tempMap.toString()));

                        System.out.println("");

                    }
                }


                //write prevMaxLsn, preMaxSeqval to Log_helper.txt
                BufferedWriter out = new BufferedWriter(new FileWriter(file_helper));
                out.write(prevMaxLsn + "\n");
                out.write(prevMaxSeqval+ "\n");
                out.close();

            }
        }

        catch(IOException e){
            System.out.println("create file fail!");
        }

        catch(SQLException e){
            System.out.println("SQL exception");
            System.out.println(e.getMessage());
        }

        finally{
            producer.close();
        }
    }

    private static String helpToString(Integer type, byte[] toProcess) {
        ByteBuffer wrapped = ByteBuffer.wrap(toProcess);
        switch (type) {
            case Types.TIMESTAMP:
                return Long.toString(wrapped.getLong());
            case Types.BIGINT:
                return Long.toString(wrapped.getLong());
            case Types.BINARY:
                return Long.toString(wrapped.getLong());
            case Types.BOOLEAN:
                return Boolean.toString(toProcess[0] != 0);
            case Types.CHAR:
                return "\'" + new String(toProcess) + "\'";
            case Types.DOUBLE:
                return Double.toString(wrapped.getDouble());
            case Types.FLOAT:
                return Double.toString(wrapped.getDouble());
            case Types.VARCHAR:
                return "\'" + new String(toProcess) + "\'";
            case Types.LONGNVARCHAR:
                return new String(toProcess);
            case Types.BIT:
                return Boolean.toString(toProcess[0] != 0);
            case Types.TINYINT:
                return Integer.toString((int) toProcess[0]);
            case Types.SMALLINT:
                return Short.toString(wrapped.getShort());
            case Types.INTEGER:
                return Integer.toString(wrapped.getInt());
            case Types.REAL:
                return Float.toString(wrapped.getFloat());
            default:
                return "unhandled type:" + Integer.toString(type);
            //TODO: unhandled data types and testing
        }
    }
}


