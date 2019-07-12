package com.unicom.portal.datamigr;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.queue.lingdao.*;
import com.unicom.portal.datamigr.queue.url.PendUrlConsumer;
import com.unicom.portal.datamigr.queue.url.PendUrlProducer;
import com.unicom.portal.datamigr.queue.zengl.ZlPendProducer;
import com.unicom.portal.datamigr.queue.zengl.ZlReadProducer;
import com.unicom.portal.datamigr.service.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MigrMain {

    public static void main(String[] args) throws ClassNotFoundException {
        if (args.length <= 0) {
            System.out.println("无参数...");
            return;
        }

        Map<String, String> meta = new HashMap<>();
        meta.put("cName", args[0]);
        meta.put("esNodes", args[1]);
        meta.put("user", args[2]);
        meta.put("password", args[3]);
        meta.put("url", args[4]);

        System.out.println(new Gson().toJson(meta));

        EsClient.initClient(meta);
        try {
            String pendCount = args[5];
            MigrConst.TBL.TBL_PEND_COUNT = Integer.parseInt(pendCount);
            String readCount = args[6];
            MigrConst.TBL.TBL_READ_COUNT = Integer.parseInt(readCount);
            if (MigrConst.TBL.TBL_PEND_COUNT <= 0 || MigrConst.TBL.TBL_READ_COUNT <= 0) {
                System.out.println("err::TBL_PEND_COUNT::" + pendCount + "::TBL_READ_COUNT::" + readCount);
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        String driver = "com.mysql.jdbc.Driver";
        Class.forName(driver);
        String url = meta.get("url");
        String user = meta.get("user");
        String password = meta.get("password");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }

        MonitorService monitorService = new MonitorService();
        monitorService.monitorToES();

        Thread pendProducerThread = new Thread(new ZlPendProducer(conn, "ZlPendProducer"));
        pendProducerThread.start();

        Thread readProducerThread = new Thread(new ZlReadProducer(conn, "ZlReadProducer"));
        readProducerThread.start();
    }

}