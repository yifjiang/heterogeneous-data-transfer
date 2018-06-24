package com.datayes.heterDataTransfer.sync;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.BitSet;

public class SQLServerComparater {
    Connection con;

    SQLServerComparater() throws ClassNotFoundException, SQLException {
        ServerConfig serverConfig = new ServerConfig();
        Class.forName(serverConfig.getSqlClass());
        String connectionUrl = serverConfig.getSqlConnectionUrl();
        con = DriverManager.getConnection(connectionUrl);
    }

    Connection getConnection(){
        return con;
    }

    long getAggregatedTimestamp(
            String tableName,
            String aggregation)
            throws SQLException
    {
        String query = "SELECT "+aggregation+"(TMSTAMP) FROM "+tableName;
        Statement stmt = con.createStatement();
        ResultSet rst = stmt.executeQuery(query);
        if (rst.next()){
            long ret = ByteBuffer.wrap(rst.getBytes(1)).getLong();
            if (rst.wasNull()) return -1;
            return ret;
        }else{
            return -1;
        }
    }

    long getMinTimestamp(String tableName) throws SQLException {
        return getAggregatedTimestamp(tableName, "MIN");
    }

    long getMaxTimestamp(String tableName) throws SQLException {
        return getAggregatedTimestamp(tableName, "MAX");
    }

    DataSet calculateDifference(
            String tableName,
            long begin,
            long end,
            BitSet clientHas)
            throws SQLException
    {
        Statement stmt = con.createStatement();
        System.out.println(String.format("SELECT * FROM %s WHERE TMSTAMP >= %s AND TMSTAMP < %s;",
                tableName,
                Long.toString(begin),
                Long.toString(end)));
        ResultSet rst = stmt.executeQuery(
                String.format("SELECT * FROM %s WHERE TMSTAMP >= %s AND TMSTAMP < %s;",
                        tableName,
                        Long.toString(begin),
                        Long.toString(end))
        );

        DataSet dataSet = new DataSet();
        String debugMessage = dataSet.constructFrom(rst, clientHas, begin);

        System.out.println(debugMessage);

        return dataSet;
    }

}
