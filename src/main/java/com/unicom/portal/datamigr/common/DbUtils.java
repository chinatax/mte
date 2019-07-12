package com.unicom.portal.datamigr.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by duanzj on 2019/4/25.
 */
public class DbUtils {

    public static Connection getConnection(Map<String, String> meta) throws Exception {
        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            String url = meta.get("url");
            String user = meta.get("user");
            String password = meta.get("password");
            Connection conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
            }
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("数据库异常");
        }
    }

    public static void close(Connection conn){
        if (conn !=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
