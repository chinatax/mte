//package com.unicom.portal.datamigr.queue.lingdao;
//
//import com.google.gson.Gson;
//import com.unicom.portal.datamigr.common.EsClient;
//import com.unicom.portal.datamigr.common.MigrConst;
//import com.unicom.portal.datamigr.common.PendUtils;
//import com.unicom.portal.datamigr.entity.HistPendingEntity;
//import org.apache.commons.collections.CollectionUtils;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Created by zhijund on 2019/4/30.
// */
//public class LdPendProducerX implements Runnable {
//    private String threadName;
//
//    private String tblNamePrefix = "pending_";
//
//    private Connection conn;
//
//    public LdPendProducerX(Connection conn, String threadName) {
//        this.conn = conn;
//        this.threadName = threadName;
//    }
//
//    @Override
//    public void run() {
//        System.out.println(threadName + "::启动...");
//        try {
//            for (int j = 0; j < MigrConst.TBL.TBL_PEND_COUNT; j++) {
//                String tableName = tblNamePrefix + j;
//                System.out.println("TBL_PEND::tblName::" + tableName);
//                Statement statement = conn.createStatement();
//                String countSql = "SELECT COUNT(1) total FROM " + tableName + " where pendingUserID = 'sunsz' ";
//                ResultSet rsCount = statement.executeQuery(countSql);
//                ResultSet rs = null;
//                int count = 0;
//                while (rsCount.next()) {
//                    count = Integer.parseInt(rsCount.getString("total"));
//                }
//                System.out.println(tableName + "::count::" + count);
//                if (count <= 0) {
//                    System.out.println(tableName + "::无数据");
//                    continue;
//                }
//                MigrConst.COUNTER.LD_P_TOTAL += count;
//                int size = 1000;
//                for (int i = 0; i < count; i += size) {
//                    if (i + size > count) {
//                        //作用为size最后没有100条数据则剩余几条newList中就装几条
//                        size = count - i;
//                    }
//                    String sql = "select * from " + tableName + " where pendingUserID='sunsz' limit " + i + ", " + size;
//                    System.out.println(tableName + "::sql::" + sql);
//                    rs = statement.executeQuery(sql);
//                    List<HistPendingEntity> lst = new ArrayList<>();
//                    while (rs.next()) {
//                        HistPendingEntity p = PendUtils.getHistPendingEntity(rs);
//                        lst.add(p);
//                    }
//                    push(lst);
//                    Thread.sleep(500);
//                }
//                rs.close();
//                rsCount.close();
//                System.out.println(tableName + "::已办数据查询完成！");
//                Thread.sleep(2000);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    void push(List<HistPendingEntity> lst) {
//        //  List<HistPendingEntity> lst = PendQueue.P_Q.poll(2, TimeUnit.SECONDS);
//        if (CollectionUtils.isEmpty(lst)) {
//            return;
//        }
//        System.out.println("threadName::" + threadName + "::接收::" + lst.size());
//        lst.forEach(v -> {
//            try {
//                String json = new Gson().toJson(v);
//                EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, v.getPendingId(), null);
//                MigrConst.COUNTER.LD_P.incrementAndGet();
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("err::PendingId::" + v.getPendingId());
//            }
//        });
//    }
//
//}