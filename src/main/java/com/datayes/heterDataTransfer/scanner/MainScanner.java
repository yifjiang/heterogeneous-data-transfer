package com.datayes.heterDataTransfer.scanner;

import com.datayes.heterDataTransfer.server.ServerConfig;

import java.sql.*;
import java.util.HashSet;

/**
 * Created by lyhdk7 on 7/7/18.
 */
public class MainScanner extends Thread {

    Connection con;

    HashSet<String> tableSet;

    public MainScanner() throws ClassNotFoundException, SQLException {
        con = DriverManager.getConnection(ServerConfig.sqlConnectionUrl);
        tableSet = new HashSet<>();
    }

    Connection getConnection(){
        return con;
    }

    public void run() {
        try {
            while(true) {
                DatabaseMetaData tableMetaData =  con.getMetaData();
                ResultSet rst = tableMetaData.getTables(null, "dbo", "%", null);
                while(rst.next()) {
                    String currentTableName = rst.getString("TABLE_NAME");
                    if (tableSet.contains(currentTableName) == false) {
                        tableSet.add(currentTableName);
                        InsertUpdateScanner insertUpdateScanner = new InsertUpdateScanner(currentTableName);
                        insertUpdateScanner.start();

                        DeleteScanner deleteScanner = new DeleteScanner(currentTableName);
                        deleteScanner.start();
                    }
                }




            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

    }

    public String createTableStatement(String tableName) {
        String stat = "DECLARE @table_name SYSNAME\nSELECT @table_name = \'" + tableName + "\'\n\nDECLARE \n      @object_name SYSNAME\n    , @object_id INT\n\nSELECT \n      @object_name = \'[\' + s.name + \'].[\' + o.name + \']\'\n    , @object_id = o.[object_id]\nFROM sys.objects o WITH (NOWAIT)\nJOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]\nWHERE s.name + \'.\' + o.name = @table_name\n    AND o.[type] = \'U\'\n    AND o.is_ms_shipped = 0\n\nDECLARE @SQL NVARCHAR(MAX) = \'\'\n\n;WITH index_column AS \n(\n    SELECT \n          ic.[object_id]\n        , ic.index_id\n        , ic.is_descending_key\n        , ic.is_included_column\n        , c.name\n    FROM sys.index_columns ic WITH (NOWAIT)\n    JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id\n    WHERE ic.[object_id] = @object_id\n),\nfk_columns AS \n(\n     SELECT \n          k.constraint_object_id\n        , cname = c.name\n        , rcname = rc.name\n    FROM sys.foreign_key_columns k WITH (NOWAIT)\n    JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id \n    JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id\n    WHERE k.parent_object_id = @object_id\n)\nSELECT @SQL = \'CREATE TABLE \' + @object_name + CHAR(32) + \'(\' + CHAR(32) + STUFF((\n    SELECT CHAR(32) + \', [\' + c.name + \'] \' + \n        CASE WHEN c.is_computed = 1\n            THEN \'AS \' + cc.[definition] \n            ELSE UPPER(tp.name) + \n                CASE WHEN tp.name IN (\'varchar\', \'char\', \'varbinary\', \'binary\', \'text\')\n                       THEN \'(\' + CASE WHEN c.max_length = -1 THEN \'MAX\' ELSE CAST(c.max_length AS VARCHAR(5)) END + \')\'\n                     WHEN tp.name IN (\'nvarchar\', \'nchar\', \'ntext\')\n                       THEN \'(\' + CASE WHEN c.max_length = -1 THEN \'MAX\' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + \')\'\n                     WHEN tp.name IN (\'datetime2\', \'time2\', \'datetimeoffset\') \n                       THEN \'(\' + CAST(c.scale AS VARCHAR(5)) + \')\'\n                     WHEN tp.name = \'decimal\' \n                       THEN \'(\' + CAST(c.[precision] AS VARCHAR(5)) + \',\' + CAST(c.scale AS VARCHAR(5)) + \')\'\n                    ELSE \'\'\n                END +\n                CASE WHEN c.collation_name IS NOT NULL THEN \' COLLATE \' + c.collation_name ELSE \'\' END +\n                CASE WHEN c.is_nullable = 1 THEN \' NULL\' ELSE \' NOT NULL\' END +\n                CASE WHEN dc.[definition] IS NOT NULL THEN \' DEFAULT\' + dc.[definition] ELSE \'\' END + \n                CASE WHEN ic.is_identity = 1 THEN \' IDENTITY(\' + CAST(ISNULL(ic.seed_value, \'0\') AS CHAR(1)) + \',\' + CAST(ISNULL(ic.increment_value, \'1\') AS CHAR(1)) + \')\' ELSE \'\' END \n        END + CHAR(32)\n    FROM sys.columns c WITH (NOWAIT)\n    JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id\n    LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id\n    LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id\n    LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id\n    WHERE c.[object_id] = @object_id\n    ORDER BY c.column_id\n    FOR XML PATH(\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\'), 1, 2, CHAR(32) + \' \')\n    + ISNULL((SELECT CHAR(32) + \', CONSTRAINT [\' + k.name + \'] PRIMARY KEY (\' + \n                    (SELECT STUFF((\n                         SELECT \', [\' + c.name + \'] \' + CASE WHEN ic.is_descending_key = 1 THEN \'DESC\' ELSE \'ASC\' END\n                         FROM sys.index_columns ic WITH (NOWAIT)\n                         JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id\n                         WHERE ic.is_included_column = 0\n                             AND ic.[object_id] = k.parent_object_id \n                             AND ic.index_id = k.unique_index_id     \n                         FOR XML PATH(N\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\'), 1, 2, \'\'))\n            + \')\' + CHAR(32)\n            FROM sys.key_constraints k WITH (NOWAIT)\n            WHERE k.parent_object_id = @object_id \n                AND k.[type] = \'PK\'), \'\') + \')\'  + CHAR(32)\n    + ISNULL((SELECT (\n        SELECT CHAR(32) +\n             \'ALTER TABLE \' + @object_name + \' WITH\' \n            + CASE WHEN fk.is_not_trusted = 1 \n                THEN \' NOCHECK\' \n                ELSE \' CHECK\' \n              END + \n              \' ADD CONSTRAINT [\' + fk.name  + \'] FOREIGN KEY(\' \n              + STUFF((\n                SELECT \', [\' + k.cname + \']\'\n                FROM fk_columns k\n                WHERE k.constraint_object_id = fk.[object_id]\n                FOR XML PATH(\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\'), 1, 2, \'\')\n               + \')\' +\n              \' REFERENCES [\' + SCHEMA_NAME(ro.[schema_id]) + \'].[\' + ro.name + \'] (\'\n              + STUFF((\n                SELECT \', [\' + k.rcname + \']\'\n                FROM fk_columns k\n                WHERE k.constraint_object_id = fk.[object_id]\n                FOR XML PATH(\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\'), 1, 2, \'\')\n               + \')\'\n            + CASE \n                WHEN fk.delete_referential_action = 1 THEN \' ON DELETE CASCADE\' \n                WHEN fk.delete_referential_action = 2 THEN \' ON DELETE SET NULL\'\n                WHEN fk.delete_referential_action = 3 THEN \' ON DELETE SET DEFAULT\' \n                ELSE \'\' \n              END\n            + CASE \n                WHEN fk.update_referential_action = 1 THEN \' ON UPDATE CASCADE\'\n                WHEN fk.update_referential_action = 2 THEN \' ON UPDATE SET NULL\'\n                WHEN fk.update_referential_action = 3 THEN \' ON UPDATE SET DEFAULT\'  \n                ELSE \'\' \n              END \n            + CHAR(32) + \'ALTER TABLE \' + @object_name + \' CHECK CONSTRAINT [\' + fk.name  + \']\' + CHAR(32)\n        FROM sys.foreign_keys fk WITH (NOWAIT)\n        JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id\n        WHERE fk.parent_object_id = @object_id\n        FOR XML PATH(N\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\')), \'\')\n    + ISNULL(((SELECT\n         CHAR(32) + \'CREATE\' + CASE WHEN i.is_unique = 1 THEN \' UNIQUE\' ELSE \'\' END \n                + \' NONCLUSTERED INDEX [\' + i.name + \'] ON \' + @object_name + \' (\' +\n                STUFF((\n                SELECT \', [\' + c.name + \']\' + CASE WHEN c.is_descending_key = 1 THEN \' DESC\' ELSE \' ASC\' END\n                FROM index_column c\n                WHERE c.is_included_column = 0\n                    AND c.index_id = i.index_id\n                FOR XML PATH(\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\'), 1, 2, \'\') + \')\'  \n                + ISNULL(CHAR(32) + \'INCLUDE (\' + \n                    STUFF((\n                    SELECT \', [\' + c.name + \']\'\n                    FROM index_column c\n                    WHERE c.is_included_column = 1\n                        AND c.index_id = i.index_id\n                    FOR XML PATH(\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\'), 1, 2, \'\') + \')\', \'\')  + CHAR(32)\n        FROM sys.indexes i WITH (NOWAIT)\n        WHERE i.[object_id] = @object_id\n            AND i.is_primary_key = 0\n            AND i.[type] = 2\n        FOR XML PATH(\'\'), TYPE).value(\'.\', \'NVARCHAR(MAX)\')\n    ), \'\')\n\nPRINT @SQL";

        String result = "";

        try {


            Statement stmt = con.createStatement();

            stmt.execute(stat);

            SQLWarning warning = stmt.getWarnings();



            while (warning != null) {
                result = warning.getMessage();
                warning = warning.getNextWarning();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return result;
        
    }



}
