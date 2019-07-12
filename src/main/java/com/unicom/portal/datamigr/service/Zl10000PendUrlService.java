//package com.unicom.portal.datamigr.service;
//
//import com.google.gson.Gson;
//import com.unicom.portal.datamigr.common.Cache;
//import com.unicom.portal.datamigr.common.EsClient;
//import com.unicom.portal.datamigr.common.MigrConst;
//
//import java.sql.*;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.MigrExecutor;
//import java.util.concurrent.Executors;
//
///**
// * Created by duanzj on 2019/3/20.
// */
//public class Zl10000PendUrlService {
//
//    private MigrExecutor executor = Executors.newFixedThreadPool(3);
//
//    public void total() {
//        Map<String, Object> queryMap = new HashMap<>();
//        queryMap.put("pendingURL", "0");
//        EsClient.hitTotal(MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, queryMap);
//    }
//
//    public void updateHisPendUrl(Map<String, String> meta) {
//        System.out.println("Zl10000PendUrlService >>>Start");
//        try {
//            String driver = "com.mysql.jdbc.Driver";
//            Class.forName(driver);
//            String url = meta.get("url");
//            String user = meta.get("user");
//            String password = meta.get("password");
//            Connection con = DriverManager.getConnection(url, user, password);
//            if (!con.isClosed()) {
//                System.out.println("Succeeded connecting to the Database!");
//            }
//            while (true) {
//                long c = MigrConst.zl10000.get();
//                if (c == 0 || (c > 0 && c % 10000 == 0)) {
//                    Map<String, Object> queryMap = new HashMap<>();
//                    queryMap.put("pendingURL", "0");
//                    List<Map<String, Object>> list = EsClient.searchForList(MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, queryMap, 10000);
//                    if (list.isEmpty()) {
//                        System.out.println("无数据....");
//                        return;
//                    }
//                    executor.submit(new WriteRunnar(list, con));
//                }
//                Thread.sleep(120000);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    class WriteRunnar implements Runnable {
//        Connection conn;
//        List<Map<String, Object>> list;
//
//        public WriteRunnar(List<Map<String, Object>> list, Connection conn) {
//            this.list = list;
//            this.conn = conn;
//        }
//
//        @Override
//        public void run() {
//            for (Map<String, Object> m : list) {
//                String srcPendingId = m.get("pendingId") + "";
//                if (Cache.IDS.containsKey(srcPendingId)) {
//                    continue;
//                } else {
//                    Cache.IDS.put(srcPendingId, srcPendingId);
//                }
//                String sql = "SELECT _id pendingId,pendingURL,`pendingUserID` FROM `pending_temp` WHERE _id = '" + srcPendingId + "'";
//                try {
//                    Statement statement = conn.createStatement();
//                    ResultSet rs = statement.executeQuery(sql);
//                    while (rs.next()) {
//                        String pendingId = rs.getString("pendingId");
//                        String pendingUserId = rs.getString("pendingUserID");
//                        String pendingURL = rs.getString("pendingURL");
//                        m.put("pendingURL", pendingURL);
//                        String json = new Gson().toJson(m);
//                        try {
//                            EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, srcPendingId, null);
//                            MigrConst.zl10000.incrementAndGet();
//                        } catch (Exception e) {
//                            System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId + "::json::" + json);
//                        }
//                    }
//                    rs.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                    System.out.println("err::srcPendingId::" + srcPendingId);
//                }
//            }
//            System.out.println("完成10000....");
//        }
//    }
//}
