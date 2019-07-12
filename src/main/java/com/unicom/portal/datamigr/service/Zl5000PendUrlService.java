//package com.unicom.portal.datamigr.service;
//
//import com.google.gson.Gson;
//import com.unicom.portal.datamigr.common.Cache;
//import com.unicom.portal.datamigr.common.EsClient;
//import com.unicom.portal.datamigr.common.IdsUtils;
//import com.unicom.portal.datamigr.common.MigrConst;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchType;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.index.query.BoolQueryBuilder;
//import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.SearchHits;
//import org.springframework.util.CollectionUtils;
//
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.MigrExecutor;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * Created by duanzj on 2019/3/20.
// */
//public class Zl5000PendUrlService {
//
//    private volatile Map<String, String> idMap = new HashMap<>();
//
//    private MigrExecutor executor = Executors.newFixedThreadPool(2);
//
//    private MigrExecutor writerExecutor = Executors.newFixedThreadPool(4);
//
//    public void updateHisPendUrl(Map<String, String> meta) {
//        System.out.println("updatePendUrlService1>>>Start");
//        executor.submit(() -> {
//            while (true) {
//                Map<String, Object> queryMap = new HashMap<>();
//                queryMap.put("pendingURL", "0");
//                long unUpdateTotal = EsClient.hitTotal(MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, queryMap);
//                System.out.println("监控::zl00000::" + MigrConst.zl10000.get() + "::zl5000::" + MigrConst.zl5000.get() + "::unUpdateTotal::" + unUpdateTotal);
//                Thread.sleep(3000);
//            }
//        });
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
//            // System.out.println("scroll 模式启动！");
//            QueryBuilder qb = QueryBuilders.boolQuery();
//            Map<String, Object> queryMap = new HashMap<>();
//            queryMap.put("pendingURL", "0");
//            for (String key : queryMap.keySet()) {
//                MatchPhraseQueryBuilder mpq1 = QueryBuilders.matchPhraseQuery(key, queryMap.get(key).toString());
//                ((BoolQueryBuilder) qb).must(mpq1);
//            }
//            SearchResponse scrollResponse = EsClient.client.prepareSearch(MigrConst.ES.HistPendDB_Index)
//                    .setTypes(MigrConst.ES.HistPendDB_type)
//                    .setQuery(qb)
//                    .setSearchType(SearchType.DEFAULT).setSize(5000).setScroll(TimeValue.timeValueMinutes(1))
//                    .execute().actionGet();
//            //第一次不返回数据
//            long count = scrollResponse.getHits().getTotalHits();
//            for (int sum = 0; sum < count; ) {
//                //System.out.println("counter::" + counter.get());
//                scrollResponse = EsClient.client.prepareSearchScroll(scrollResponse.getScrollId())
//                        .setScroll(TimeValue.timeValueMinutes(8))
//                        .execute().actionGet();
//                SearchHits hits = scrollResponse.getHits();
//                sum += hits.getHits().length;
//                System.out.println("总量" + count + " 已经查到" + sum);
//                List<Map<String, Object>> listMap = new ArrayList<Map<String, Object>>();
//                for (SearchHit hit : hits) {
//                    listMap.add(hit.getSourceAsMap());
//                }
//                executor.submit(new WriteRunnar(listMap, con));
//                Thread.sleep(60000);
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
//            Map<String, Map<String, Object>> pendMap = new HashMap<>();
//            list.forEach(m -> {
//                String pid = m.get("pendingId") + "";
//                if(!Cache.IDS.containsKey(pid)) {
//                    pendMap.put(pid, m);
//                }
//            });
//            String[] ids = IdsUtils.getIds(list);
//            String inSql = IdsUtils.getSqlForIn(ids, "_id");
//            String sql = "SELECT _id pendingId,pendingURL,`pendingUserID` FROM `pending_temp` WHERE 1=1 " + inSql;
//            try {
//                Statement statement = conn.createStatement();
//                ResultSet rs = statement.executeQuery(sql);
//                while (rs.next()) {
//                    String pendingId = rs.getString("pendingId");
//                    String pendingUserId = rs.getString("pendingUserID");
//                    String pendingURL = rs.getString("pendingURL");
//                    Map<String, Object> m = pendMap.get(pendingId);
//                    if (pendMap.containsKey(pendingId)) {
//                        m.put("pendingURL", pendingURL);
//                        String json = new Gson().toJson(m);
//                        try {
//                            EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, pendingId, null);
//                            MigrConst.zl5000.incrementAndGet();
//                        } catch (Exception e) {
//                            System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId + "::json::" + json);
//                        }
//                    }
//                }
//                System.out.println("完成:" + list.size() + "条");
//                rs.close();
//            } catch (SQLException e) {
//                System.out.println("数据库异常....");
//            }
//        }
////        @Override
////        public void run() {
////            for (Map<String, Object> m : list) {
////                String srcPendingId = m.get("pendingId") + "";
////                if (idMap.containsKey(srcPendingId)) {
////                    continue;
////                } else {
////                    idMap.put(srcPendingId, srcPendingId);
////                }
////                writerExecutor.submit(() -> {
////                    String [] ids = IdsUtils.getIds(list);
////                    String inSql = IdsUtils.getSqlForIn(ids,"_id");
////                    String sql = "SELECT _id pendingId,pendingURL,`pendingUserID` FROM `pending_temp` WHERE _id = '" + srcPendingId + "'";
////                    try {
////                        Statement statement = conn.createStatement();
////                        ResultSet rs = statement.executeQuery(sql);
////                        while (rs.next()) {
////                            String pendingId = rs.getString("pendingId");
////                            String pendingUserId = rs.getString("pendingUserID");
////                            String pendingURL = rs.getString("pendingURL");
////                            m.put("pendingURL", pendingURL);
////                            String json = new Gson().toJson(m);
////                            try {
////                                EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, srcPendingId, null);
////                                counter.incrementAndGet();
////                            } catch (Exception e) {
////                                System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId + "::json::" + json);
////                            }
////                        }
////                        rs.close();
////                    } catch (SQLException e) {
////                        e.printStackTrace();
////                        System.out.println("err::srcPendingId::" + srcPendingId);
////                    }
////                });
////            }
////        }
//
//    }
//}
