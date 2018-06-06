import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestMssqlConnection {
    public static void main(String [] args){
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            String connectionUrl = "jdbc:sqlserver://localhost:1433;" +
                    "databaseName=testMSSQL;" +
                    "user=sa;password=reallyStrongPwd123;";
            Connection con = DriverManager.getConnection(connectionUrl);
            String query1 = "IF Object_id('news_detail_backup', 'U')" +
                    " IS NOT NULL\n" +
                    "    DROP TABLE news_detail_backup;";
            String query2 = "CREATE TABLE " +
                    "\"news_detail_backup\" (\n" +
                    "\t\"news_id\" BIGINT NOT NULL,\n" +
                    "\t\"news_body\" TEXT NULL DEFAULT NULL,\n" +
                    "\t\"insert_time\" DATETIME NULL DEFAULT NULL,\n" +
                    "\t\"update_time\" DATETIME NULL DEFAULT NULL,\n" +
                    "\t\"source_name\" NVARCHAR(50) NULL DEFAULT NULL,\n" +
                    "\t\"news_author\" NVARCHAR(500) NULL DEFAULT NULL,\n" +
                    "\t\"news_url\" VARCHAR(500) NULL DEFAULT NULL,\n" +
                    "\t\"news_title\" NVARCHAR(300) NULL DEFAULT NULL,\n" +
                    "\t\"news_fetch_time\" DATETIME2(7) NULL DEFAULT NULL,\n" +
                    "\t\"news_site_name\" NVARCHAR(50) NULL DEFAULT NULL,\n" +
                    "\t\"news_publish_time\" DATETIME2(7) NULL DEFAULT NULL,\n" +
                    "\t\"news_comments_num\" INT NULL DEFAULT NULL,\n" +
                    "\t\"hashValue\" VARCHAR(200) NULL DEFAULT NULL,\n" +
                    "\t\"has_summary\" INT NULL DEFAULT NULL,\n" +
                    "\t\"isActive\" INT NULL DEFAULT '1',\n" +
                    "\t\"source_type\" VARCHAR(64) NULL DEFAULT NULL,\n" +
                    "\t\"news_tag\" INT NULL DEFAULT '-1',\n" +
                    "\t\"related_company_id\" VARCHAR(500) NULL DEFAULT NULL,\n" +
                    "\t\"main_related_company_id\" BIGINT NULL DEFAULT NULL,\n" +
                    "\t\"event_id\" INT NULL DEFAULT '-1',\n" +
                    "\t\"neg_pos\" FLOAT(53) NULL DEFAULT NULL,\n" +
                    "\t\"duplicate_num\" INT NULL DEFAULT '0',\n" +
                    "\t\"kpi_tag\" VARCHAR(500) NULL DEFAULT NULL,\n" +
                    "\t\"comments_num\" INT NULL DEFAULT NULL,\n" +
                    "\t\"kpi_type\" INT NULL DEFAULT '-1',\n" +
                    "\t\"usefull\" INT NULL DEFAULT '1',\n" +
                    "\t\"group_id\" BIGINT NOT NULL DEFAULT '-1',\n" +
                    "\t\"auto_update_time\" DATETIME2(7) NOT NULL DEFAULT '',\n" +
                    "\t\"page_tag\" NVARCHAR(150) NULL DEFAULT NULL,\n" +
                    "\t\"TMSTAMP\" TIMESTAMP NOT NULL,\n" +
                    "\tPRIMARY KEY (\"news_id\"),\n" +
//                    "\tUNIQUE INDEX \"UNIQUE\" (\"TMSTAMP\")\n" +
                    ")\n" +
                    ";";
            String query3 = "IF Object_id('news_detail_backup', 'U')" +
                    " IS NOT NULL\n" +
                    "    DROP TABLE mkt_equd_adj;";
            String query4 = "CREATE TABLE \"mkt_equd_adj\" (\n" +
                    "\t\"ID\" BIGINT NOT NULL,\n" +
                    "\t\"SECURITY_ID\" BIGINT NOT NULL,\n" +
                    "\t\"TICKER_SYMBOL\" VARCHAR(20) NOT NULL,\n" +
                    "\t\"EXCHANGE_CD\" CHAR(4) NULL DEFAULT NULL,\n" +
                    "\t\"TRADE_DATE\" DATE NOT NULL,\n" +
                    "\t\"ACT_PRE_CLOSE_PRICE\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"OPEN_PRICE\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"HIGHEST_PRICE\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"LOWEST_PRICE\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"CLOSE_PRICE\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"PRE_CLOSE_PRICE\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"PRE_CLOSE_PRICE_1\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"OPEN_PRICE_1\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"HIGHEST_PRICE_1\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"LOWEST_PRICE_1\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"CLOSE_PRICE_1\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"ETL_PRE_CLOSE_PRICE\" DECIMAL(9,3) NULL DEFAULT NULL,\n" +
                    "\t\"ADJ_FACTOR_1\" NUMERIC(15,10) NULL DEFAULT NULL,\n" +
                    "\t\"End_Date\" DATE NULL DEFAULT NULL,\n" +
                    "\t\"EX_DIV_DATE\" DATE NULL DEFAULT NULL,\n" +
                    "\t\"ACCUM_ADJ_FACTOR\" NUMERIC(15,10) NULL DEFAULT NULL,\n" +
                    "\t\"ETL_CRC\" BIGINT NULL DEFAULT NULL,\n" +
                    "\t\"QA_RULE_CHK_FLG\" TINYINT NULL DEFAULT NULL,\n" +
                    "\t\"QA_MANUAL_FLG\" BIT NULL DEFAULT NULL,\n" +
                    "\t\"QA_ACTIVE_FLG\" BIT NOT NULL,\n" +
                    "\t\"CREATE_BY\" VARCHAR(50) NULL DEFAULT NULL,\n" +
                    "\t\"CREATE_TIME\" DATETIME NOT NULL,\n" +
                    "\t\"UPDATE_BY\" VARCHAR(50) NULL DEFAULT NULL,\n" +
                    "\t\"UPDATE_TIME\" DATETIME NULL DEFAULT NULL,\n" +
                    "\t\"TMSTAMP\" TIMESTAMP NOT NULL,\n" +
//                    "\tUNIQUE INDEX \"UNIQUE\" (\"SECURITY_ID\", \"TRADE_DATE\"),\n" +
                    "\tPRIMARY KEY (\"ID\"),\n" +
//                    "\tUNIQUE INDEX \"UNIQUE\" (\"TMSTAMP\")\n" +
                    ")\n" +
                    ";\n";
            String query5 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_NAME = 'news_detail_backup'";
            Statement stmt = con.createStatement();
            stmt.execute(query1);
            stmt.execute(query2);
            stmt.execute(query3);
            stmt.execute(query4);
            ResultSet rst = stmt.executeQuery(query5);
            while (rst.next()) {
                System.out.println(rst.getString("COLUMN_NAME"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
