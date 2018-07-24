package com.datayes.heterDataTransfer.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;

public class Helper {
    public static String toString(Integer type, byte[] toProcess) {
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
                return "\'" + new String(toProcess) + "\'";
            case Types.DECIMAL:
                BigInteger bi = new BigInteger(1, toProcess);
                BigDecimal bd = new BigDecimal(bi);
                return bd.toString();
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

    public static Object toObject(int type, byte[] data){
        ByteBuffer wrapped = ByteBuffer.wrap(data);
        switch (type) {
            case Types.TIMESTAMP:
                return wrapped.getLong();
            case Types.BIGINT:
                return wrapped.getLong();
            case Types.BINARY:
                return wrapped.getLong();
            case Types.BOOLEAN:
                return data[0] != 0;
            case Types.CHAR:
                return new String(data);
            case Types.DOUBLE:
                return wrapped.getDouble();
            case Types.FLOAT:
                return wrapped.getDouble();
            case Types.VARCHAR:
                return new String(data);
            case Types.LONGNVARCHAR:
                return new String(data);
            case Types.BIT:
                return data[0]!=0;
            case Types.TINYINT:
                return (int)data[0];
            case Types.SMALLINT:
                return wrapped.getShort();
            case Types.INTEGER:
                return wrapped.getInt();
            case Types.REAL:
                return wrapped.getFloat();
            default:
                return "unhandled type:"+Integer.toString(type);//TODO: unhandled data types and testing
        }
    }
}
