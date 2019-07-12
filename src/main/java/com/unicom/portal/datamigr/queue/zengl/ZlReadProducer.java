package com.unicom.portal.datamigr.queue.zengl;

import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.common.ReadUtils;
import com.unicom.portal.datamigr.entity.HistReadingEntity;
import com.unicom.portal.datamigr.queue.MigrExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhijund on 2019/4/30.
 */
public class ZlReadProducer implements Runnable {
    private String threadName;

    private String tblNamePrefix = "reading_";

    private Connection conn;

    public ZlReadProducer(Connection conn, String threadName) {
        this.conn = conn;
        this.threadName = threadName;
    }

    @Override
    public void run() {
        System.out.println(threadName + "::启动...");
        for (int j = 0; j < MigrConst.TBL.TBL_READ_COUNT; j++) {
            try {
                String tableName = tblNamePrefix + j;
                System.out.println("TBL_READ::tblName::" + tableName);
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
                    System.out.println(tableName + "::无数据");
                    continue;
                }
                MigrConst.COUNTER.LD_R_TOTAL.addAndGet(count);
                int size = 1000;
                for (int i = 0; i < count; i += size) {
                    if (i + size > count) {
                        size = count - i;
                    }
                    String sql = "select * from " + tableName + " limit " + i + ", " + size;
                    System.out.println(tableName + "::sql::" + sql);
                    rs = statement.executeQuery(sql);
                    List<HistReadingEntity> lst = new ArrayList<>();
                    while (rs.next()) {
                        HistReadingEntity r = ReadUtils.getHistReadingEntity(rs);
                        lst.add(r);
                    }
                    MigrExecutor.ROR.submit(new ZlReadConsumer(lst));
                    Thread.sleep(2000);
                }
                rs.close();
                rsCount.close();
                System.out.println(tableName + "::已办数据查询完成！");
                Thread.sleep(3000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}