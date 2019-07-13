package com.dunzung.mte.migration;

import com.dunzung.mte.common.Const;
import com.dunzung.mte.utils.PendUtils;
import com.dunzung.mte.entity.HistPendingEntity;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 已办数据生产者线程
 * Created by Wooola on 2019/4/30.
 */
public class ZlPendProducer implements Runnable {

    private String threadName;

    private String tblNamePrefix = "pending_";

    private Connection conn;

    public ZlPendProducer(Connection conn, String threadName) {
        this.conn = conn;
        this.threadName = threadName;
    }

    @Override
    public void run() {
        System.out.println(threadName + "::启动...");
        for (int j = 0; j < Const.TBL.TBL_PEND_COUNT; j++)
            try {
                String tableName = tblNamePrefix + j;
                System.out.println("TBL_PEND::tblName::" + tableName);
                Statement statement = conn.createStatement();
                String countSql = "SELECT COUNT(1) total FROM " + tableName;
                ResultSet rsCount = statement.executeQuery(countSql);
                ResultSet rs = null;
                int count = 0;
                while (rsCount.next()) {
                    count = Integer.parseInt(rsCount.getString("total"));
                }
                System.out.println(tableName + "::count::" + count);
                if (count <= 0) {
                    System.out.println(tableName + "::无数据，退出线程");
                    return;
                }
                Const.COUNTER.LD_P_TOTAL.addAndGet(count);
                int size = 1000;
                for (int i = 0; i < count; i += size) {
                    if (i + size > count) {
                        //作用为size最后没有100条数据则剩余几条newList中就装几条
                        size = count - i;
                    }
                    String sql = "select * from " + tableName + " limit " + i + ", " + size;
                    System.out.println(tableName + "::sql::" + sql);
                    rs = statement.executeQuery(sql);
                    List<HistPendingEntity> lst = new ArrayList<>();
                    while (rs.next()) {
                        HistPendingEntity p = PendUtils.getHistPendingEntity(rs);
                        lst.add(p);
                    }
                    MteExecutor.POR.submit(new ZlPendConsumer(lst));
                    Thread.sleep(2000);
                }
                rs.close();
                rsCount.close();
                System.out.println(tableName + "::已办数据查询完成！");
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

}