package com.unicom.portal.datamigr.service;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by duanzj on 2019/3/20.
 */
public class UpdatePendService {

    private int counter;
    private boolean isBreak = false;

    private ExecutorService executor = Executors.newFixedThreadPool(40);

    public void updateHisPend(Map<String, String> meta) {
        System.out.println("updateHisPend>>>Start");
        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            String url = meta.get("url");
            String user = meta.get("user");
            String password = meta.get("password");
            Connection con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
            }
            Statement statement = con.createStatement();
            // String countSql = "SELECT COUNT(1)  cou FROM pending_temp";
            // ResultSet rsCount = statement.executeQuery(countSql);
            ResultSet rs = null;
            int count = 41309412;
//            while (rsCount.next()) {
//                count = Integer.parseInt(rsCount.getString("cou"));
//            }
            System.out.println("pend count>>" + 41309412);
            int size = 5000;
            for (int i = 38010000; i < count; i += size) {
                if (i + size > count) {//作用为size最后没有100条数据则剩余几条newList中就装几条
                    size = count - i;
                }
                String sql = "SELECT pendingURL,pendingUserID,_id pendingId FROM pending_temp  limit " + i + "," + size;
                System.out.println("i::" + i + "::sql::" + sql);
                try {
                    if (isBreak) {
                        break;
                    }
                    rs = statement.executeQuery(sql);
                    counter = i;
                } catch (Exception e) {
                    System.out.println("dberr::counter::" + counter + "::time:" + (new java.util.Date()).toLocaleString());
                    break;
                }
                while (rs.next()) {
                    String pendingId = rs.getString("pendingId");
                    String pendingUserId = rs.getString("pendingUserID");
                    String pendingURL = rs.getString("pendingURL");
                    executor.execute(() -> {
                        try {
                            Map<String, Object> queryMap = new HashMap<>();
                            queryMap.put("pendingId", pendingId);
                            queryMap.put("pendingUserID", pendingUserId);
                            List<Map<String, Object>> list = EsClient.searchForList(MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, queryMap, 10000);
                            for (Map<String, Object> m : list) {
                                String srcPendingId = m.get("pendingId") + "";
                                if (srcPendingId.equals(pendingId)) {
                                    m.put("pendingURL", pendingURL);
                                }
                                String json = JSONObject.toJSONString(m);
                                System.out.println(json);
                                try {
                                    EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, srcPendingId, null);
                                } catch (Exception e1) {
                                    System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId + "::json::" + json);
                                }
                            }
                        } catch (Exception e) {
                            System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId);
                            isBreak = true;
                        }
                    });
                }
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            rs.close();
            con.close();
            System.out.println("updateHisPend同步ES成功！！");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
