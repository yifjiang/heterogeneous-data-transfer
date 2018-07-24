package com.datayes.heterDataTransfer.sync;

import com.datayes.heterDataTransfer.util.Helper;

import java.math.BigDecimal;
import java.math.BigInteger;
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

    String toString(Integer type, byte[] toProcess) {
        if (toProcess == null) return "";
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
                return new String(toProcess);
            case Types.DECIMAL:
                BigInteger bi = new BigInteger(1, toProcess);
                BigDecimal bd = new BigDecimal(bi);
                return bd.toString();
            case Types.DOUBLE:
                return Double.toString(wrapped.getDouble());
            case Types.FLOAT:
                return Double.toString(wrapped.getDouble());
            case Types.VARCHAR:
                return new String(toProcess);
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

    Object toObject(int type, byte[] data){
        return Helper.toObject(type, data);
    }

    void clearHeadTail(String tableName, long min, long max) throws SQLException{
        Statement stmt = con.createStatement();
        String deleteQuery = String.format(
                "DELETE FROM %s WHERE TMSTAMP < %d OR TMSTAMP > %d",
                tableName,
                min,
                max
        );
        System.out.println(deleteQuery);
        stmt.execute(deleteQuery);
    }

    void updateTable(String tableName, DataSet dataSet, long begin) throws SQLException{
        int tmstampCol = -1, idCol = -1;
        for (int i = 0; i < dataSet.numCol; ++i){
            if (dataSet.columnNames.get(i).equals("TMSTAMP")) tmstampCol = i;
            if (dataSet.columnNames.get(i).equals("ID")) idCol = i;
        }

        StringBuilder updateQuery = new StringBuilder("UPDATE "+tableName+" SET ");
        for (int col = 0; col < dataSet.numCol; ++col){
            updateQuery.append(String.format(
                    "%s = ?",
                    dataSet.columnNames.get(col)
            ));
            if (col+1 != dataSet.numCol) updateQuery.append(", ");
        }
        updateQuery.append(" WHERE ID = ?");
        PreparedStatement pstmtUpdate = con.prepareStatement(updateQuery.toString());

        StringBuilder insertQuery = new StringBuilder("INSERT INTO "+tableName+" (");
        for (int col = 0; col < dataSet.numCol; ++col){
            insertQuery.append(dataSet.columnNames.get(col));
            if (col+1 != dataSet.numCol) insertQuery.append(", ");
        }
        insertQuery.append(") VALUE (");
        for (int col = 0; col < dataSet.numCol; ++col){
            insertQuery.append("?");
            if (col+1 != dataSet.numCol) insertQuery.append(", ");
        }
        insertQuery.append(")");
        PreparedStatement pstmtInsert = con.prepareStatement(insertQuery.toString());

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
            if (rst.next()){
                for (int col = 0; col < dataSet.numCol; ++col){
//                    pstmtUpdate.setObject(col+1,
//                            toObject(
//                                dataSet.columnTypes.get(col),
//                                dataSet.data.get(row).get(col)
//                            ),
//                            dataSet.columnTypes.get(col)
//                    );
                    pstmtUpdate.setString(
                            col+1,
                                toString(
                                    dataSet.columnTypes.get(col),
                                    dataSet.data.get(row).get(col)
                                )
                            );
                }
//                pstmtUpdate.setObject(
//                            dataSet.numCol+1,
//                            toObject(
//                                    dataSet.columnTypes.get(idCol),
//                                    dataSet.data.get(row).get(idCol)
//                            ),
//                            dataSet.columnTypes.get(idCol)
//                        );
                pstmtUpdate.setString(
                        dataSet.numCol+1,
                        toString(
                                dataSet.columnTypes.get(idCol),
                                dataSet.data.get(row).get(idCol)
                        )
                );
                System.out.println(pstmtUpdate);
                pstmtUpdate.execute();
            }else{
                for (int col = 0; col < dataSet.numCol; ++col){
//                    pstmtInsert.setObject(
//                            col + 1,
//                            toObject(
//                                    dataSet.columnTypes.get(col),
//                                    dataSet.data.get(row).get(col)
//                            ),
//                            dataSet.columnTypes.get(col)
//                    );
                    pstmtInsert.setString(
                            col + 1,
                            toString(
                                    dataSet.columnTypes.get(col),
                                    dataSet.data.get(row).get(col)
                            )
                    );
                }
                System.out.println(pstmtInsert);
                pstmtInsert.execute();
            }
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
