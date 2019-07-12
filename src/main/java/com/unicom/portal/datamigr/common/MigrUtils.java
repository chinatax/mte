package com.unicom.portal.datamigr.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Created by duanzj on 2019/4/28.
 */
public class MigrUtils {

    public static boolean hasTrue(String pendingURL) {
        if (MigrConst.PEND_URL_0.equals(pendingURL)
                || MigrConst.PEND_URL_1.equals(pendingURL)
                || MigrConst.PEND_URL_2.equals(pendingURL)) {
            return true;
        }
        return false;
    }

    public static void insertToDB(Connection conn, List<Map<String, Object>> listMap) {
        listMap.forEach(m -> {
            try {
                String pendingId = m.get("pendingId") + "";
                String pendingURL = m.get("pendingURL") + "";
                String pendingUserID = m.get("pendingUserID") + "";
                String insertSql = "INSERT INTO `pend_undo` VALUES('" + pendingId + "','" + pendingURL + "','" + pendingUserID + "')";
                Statement statement = conn.createStatement();
                statement.executeUpdate(insertSql);
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

    }

}
