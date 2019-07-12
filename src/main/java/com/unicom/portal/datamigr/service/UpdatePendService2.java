package com.unicom.portal.datamigr.service;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by duanzj on 2019/3/20.
 */
public class UpdatePendService2 {

    private ExecutorService executor = Executors.newFixedThreadPool(40);

    public void updateHisPend1(Map<String, String> meta) {
        List<String> ulist = Arrays.asList("maiyanzhou","liangbj","zhukb","fanyunjun","jizhe5","liud28","zhangzh396","suny46");
        int c = 0;
        for (String uid : ulist) {
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("pendingUserID", uid);
            List<Map<String, Object>> list = EsClient.searchForList(MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, queryMap, 10000);
//            for (int i = 0; i < 5; i++) {
//                System.out.println(list.get(i));
//            }
            c += list.size();
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>uid::" + uid + "::" + list.size());
        }
        System.out.println("合计：" + c);

    }


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
            String sql = "SELECT pendingURL,pendingUserID,_id pendingId FROM pending_temp " +
                    "where _id IN('maiyanzhou','liangbj','zhukb','fanyunjun','jizhe5','liud28','zhangzh396','suny46') ";
            System.out.println("sql::" + sql);
            ResultSet rs = null;
            try {
                rs = statement.executeQuery(sql);
            } catch (Exception e) {
                System.out.println("dberr::time:" + (new java.util.Date()).toLocaleString());
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
                        System.out.println(list.size());
                        System.out.println("pendingId::" + pendingId + "::pendingUserId::" + pendingUserId + "::size::" + list.size());
                        for (Map<String, Object> m : list) {
                            String srcPendingId = m.get("pendingId") + "";
                            m.put("pendingURL", pendingURL);
                            String json = new Gson().toJson(m);
                            System.out.println(json);
                            EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, srcPendingId, null);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId);
                    }
                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
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
