import com.datayes.heterDataTransfer.server.ServerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ExpoDemo implements Runnable {


    public void run(){
        try {
            Connection con = DriverManager.getConnection(ServerConfig.sqlConnectionUrl);
            Statement stmt = con.createStatement();
            while (true){
                stmt.execute("UPDATE test SET TICKER_SYMBOL = 'XSHE' WHERE ID = 100000004");
                System.out.println("here");
//                    "INSERT INTO test(ID, SECURITY_ID, TICKER_SYMBOL, EXCHANGE_CD, TRADE_DATE, PRE_CLOSE_PRICE,
// ACT_PRE_CLOSE_PRICE, OPEN_PRICE, HIGHEST_PRICE, LOWEST_PRICE, CLOSE_PRICE, TURNOVER_VOL, TURNOVER_VALUE, DEAL_AMOUNT,
// PE, PE1, PB, NEG_MARKET_VALUE, MARKET_VALUE, CHG_PCT, RANGE_PCT, TURNOVER_RATE , ETL_CRC, ETL_CLOSE_PRICE, QA_RULE_CHK_FLG,
// QA_MANUAL_FLG, QA_ACTIVE_FLG, CREATE_BY, CREATE_TIME, UPDATE_BY, UPDATE_TIME, TMSTAMP) VALUES (
// 100000004, 545, '200468', 'XSHE', '2010-04-01', 4.82, 4.82, 0, 0, 0, 4.82, 4.82, 4.82, 1, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 22870393, NULL, NULL, NULL, 'True', 'ETL', '2018-05-31 15:07:00.620', NULL, '2018-05-31 15:07:03.607', DEFAULT);"
                try {
                    Thread.currentThread().sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args){
        new ExpoDemo().run();
    }

}
