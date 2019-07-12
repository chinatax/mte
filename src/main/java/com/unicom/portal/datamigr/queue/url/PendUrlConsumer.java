package com.unicom.portal.datamigr.queue.url;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.IdsUtils;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.queue.PendQueue;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhijund on 2019/4/27.
 */
public class PendUrlConsumer implements Runnable {
    private String threadName;

    private Connection conn;

    public PendUrlConsumer(Connection conn, String threadName) {
        this.conn = conn;
        this.threadName = threadName;
    }

    @Override
    public void run() {
        System.out.println(threadName + "启动...");
        try {
            while (true) {
                List<Map<String, Object>> pendList = PendQueue.P_U_Q.poll(2, TimeUnit.SECONDS);
                if (CollectionUtils.isEmpty(pendList)) {
                    continue;
                }
                System.out.println("ThreadName::" + threadName + "::接收::" + pendList.size() + "条");
                Map<String, Map<String, Object>> pendMap = new HashMap<>();
                for (Map<String, Object> m : pendList) {
                    String pid = m.get("pendingId") + "";
                    pendMap.put(pid, m);
                }
                String[] ids = IdsUtils.getIds(pendList);
                String inSql = IdsUtils.getSqlForIn(ids, "_id");
                String sql = "SELECT _id pendingId,pendingURL,`pendingUserID` FROM `pending_temp` WHERE 1=1 " + inSql;
                try {
                    Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(sql);
                    while (rs.next()) {
                        String pendingId = rs.getString("pendingId");
                        String pendingUserId = rs.getString("pendingUserID");
                        String pendingURL = rs.getString("pendingURL");
                        Map<String, Object> m = pendMap.get(pendingId);
                        m.put("pendingURL", pendingURL);
                        String json = new Gson().toJson(m);
                        try {
                            EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, pendingId, null);
                            MigrConst.PEND_COUNTER.incrementAndGet();
                        } catch (Exception e) {
                            System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId + "::json::" + json);
                        }
                    }
                    System.out.println("ThreadName::" + threadName + "::完成::" + pendList.size() + "条");
                    rs.close();
                    Thread.sleep(1000);
                } catch (SQLException e) {
                    System.out.println("数据库异常....");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("PendUrlConsumer退出线程...");
        }

    }

}
