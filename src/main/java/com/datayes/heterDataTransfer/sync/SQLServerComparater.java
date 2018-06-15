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
