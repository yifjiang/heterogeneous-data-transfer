package com.datayes.heterDataTransfer.sync;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.BitSet;
import java.sql.Types;

public class MySQLConnector {

    Connection con;

    MySQLConnector() throws ClassNotFoundException, SQLException{
        ClientConfig clientConfig = new ClientConfig();
        Class.forName(clientConfig.getSqlClass());
        String connectionUrl = clientConfig.getSqlConnectionUrl();
        con = DriverManager.getConnection(connectionUrl);
    }

    Connection getConnection(){
        return con;
    }

    BitSet getTimeStamps(String tableName, long begin, long end) throws SQLException{
        assert end-begin <= Integer.MAX_VALUE;
        BitSet ret = new BitSet((int)(end-begin));
        ret.clear();
        Statement stmt = con.createStatement();
        ResultSet rst = stmt.executeQuery(String.format("SELECT TMSTAMP FROM %s WHERE TMSTAMP >= %d AND TMSTAMP <%d;",
                tableName,
                begin,
                end));
        while (rst.next()){
            long timeStamp = rst.getLong("TMSTAMP");
            assert timeStamp-begin <= Integer.MAX_VALUE;
            ret.set((int)(timeStamp-begin));
        }
        return ret;
    }

    String toString(int type, byte[] data){
        ByteBuffer wrapped = ByteBuffer.wrap(data);
        switch (type) {
            case Types.TIMESTAMP:
                return Long.toString(wrapped.getLong());
            case Types.BIGINT:
                return Long.toString(wrapped.getLong());
            case Types.BINARY:
                return Long.toString(wrapped.getLong());
        }
        return "error"+Integer.toString(type);//TODO
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
            long ret = rst.getLong(1);
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

    void updateTable(String tableName, DataSet dataSet, long begin) throws SQLException{
        int tmstampCol = -1, idCol = -1;
        for (int i = 0; i < dataSet.numCol; ++i){
            if (dataSet.columnNames.get(i).equals("TMSTAMP")) tmstampCol = i;
            if (dataSet.columnNames.get(i).equals("ID")) idCol = i;
        }
        Statement stmt = con.createStatement();
        for (int row = 0; row < dataSet.numRow; ++row){
            String tmstamp = toString(
                    Types.BIGINT,
                    dataSet.data.get(row).get(tmstampCol));
            String id = toString(
                    dataSet.columnTypes.get(idCol),
                    dataSet.data.get(row).get(idCol));
            String selectQuery = String.format(
                    "SELECT ID FROM %s WHERE ID = %s",
                    tableName,
                    id);
            ResultSet rst = stmt.executeQuery(selectQuery);
            StringBuilder updateInsertQuery;
            if (rst.next()){
                updateInsertQuery
                        = new StringBuilder("UPDATE "+tableName+" SET ");
                for (int col = 0; col < dataSet.numCol; ++col){
                    updateInsertQuery.append(String.format(
                            "%s = %s",
                            dataSet.columnNames.get(col),
                            toString(
                                    dataSet.columnTypes.get(col),
                                    dataSet.data.get(row).get(col)
                            )
                    ));
                    if (col+1 != dataSet.numCol) updateInsertQuery.append(", ");
                }
                updateInsertQuery.append(" WHERE ID = "+id);
            }else{
                updateInsertQuery = new StringBuilder("INSERT INTO "+tableName+" (");
                for (int col = 0; col < dataSet.numCol; ++col){
                    updateInsertQuery.append(dataSet.columnNames.get(col));
                    if (col+1 != dataSet.numCol) updateInsertQuery.append(", ");
                }
                updateInsertQuery.append(") VALUE (");
                for (int col = 0; col < dataSet.numCol; ++col){
                    updateInsertQuery.append(
                            toString(
                                    dataSet.columnTypes.get(col),
                                    dataSet.data.get(row).get(col)
                            )
                    );
                    if (col+1 != dataSet.numCol) updateInsertQuery.append(", ");
                }
                updateInsertQuery.append(")");
            }
            System.out.println(updateInsertQuery);
            stmt.execute(updateInsertQuery.toString());
        }
//        System.out.println(dataSet.residue);
        for (int i = dataSet.residue.nextSetBit(0);
             i != -1;
             i = dataSet.residue.nextSetBit(i+1)){
            String deleteQuery = String.format(
                    "DELETE FROM %s WHERE TMSTAMP = %s",
                    tableName,
                    Long.toString(begin + (long) i)
            );
            System.out.println(deleteQuery);
            stmt.execute(deleteQuery);
        }
    }

}
