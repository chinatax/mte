//package com.unicom.portal.datamigr.queue.lingdao;
//
//import com.google.gson.Gson;
//import com.unicom.portal.datamigr.common.EsClient;
//import com.unicom.portal.datamigr.common.MigrConst;
//import com.unicom.portal.datamigr.common.ReadUtils;
//import com.unicom.portal.datamigr.entity.HistReadingEntity;
//import com.unicom.portal.datamigr.queue.PendQueue;
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
//public class LdReadProducerX implements Runnable {
//    private String threadName;
//
//    private String tblNamePrefix = "reading_";
//
//    private Connection conn;
//
//    public LdReadProducerX(Connection conn, String threadName) {
//        this.conn = conn;
//        this.threadName = threadName;
//    }
//
//    @Override
//    public void run() {
//        System.out.println(threadName + "::启动...");
//        for (int j = 0; j < MigrConst.TBL.TBL_READ_COUNT; j++) {
//            try {
//                String tableName = tblNamePrefix + j;
//                System.out.println("TBL_READ::tblName::" + tableName);
//                Statement statement = conn.createStatement();
//                String countSql = "SELECT COUNT(1) total FROM " + tableName + " where readingUserId = 'sunsz' ";
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
//                MigrConst.COUNTER.LD_R_TOTAL += count;
//                int size = 1000;
//                for (int i = 0; i < count; i += size) {
//                    if (i + size > count) {
//                        size = count - i;
//                    }
//                    String sql = "select * from " + tableName + " where readingUserId ='sunsz' limit " + i + ", " + size;
//                    System.out.println(tableName + "::sql::" + sql);
//                    rs = statement.executeQuery(sql);
//                    List<HistReadingEntity> lst = new ArrayList<>();
//                    while (rs.next()) {
//                        HistReadingEntity r = ReadUtils.getHistReadingEntity(rs);
//                        lst.add(r);
//                    }
//                    PendQueue.R_Q.offer(lst);
//                    //push(lst);
//                    Thread.sleep(500);
//                }
//                rs.close();
//                rsCount.close();
//                System.out.println(tableName + "::已阅数据同步ES成功！");
//                Thread.sleep(3000);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    void push(List<HistReadingEntity> lst){
//       // List<HistReadingEntity> lst = PendQueue.R_Q.poll(2, TimeUnit.SECONDS);
//        if (CollectionUtils.isEmpty(lst)) {
//            return;
//        }
//        System.out.println("threadName::" + threadName + "::接收::" + lst.size());
//        lst.forEach(v -> {
//            try {
//                String json = new Gson().toJson(v);
//                EsClient.addDataInJSON(json, MigrConst.ES.HistReadDB_Index, MigrConst.ES.HistReadDB_type, v.getReadingId(), null);
//                MigrConst.COUNTER.LD_R.incrementAndGet();
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("err::ReadingId::" + v.getReadingId());
//            }
//        });
//    }
//
//}