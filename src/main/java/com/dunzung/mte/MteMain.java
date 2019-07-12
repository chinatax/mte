package com.dunzung.mte;

import com.dunzung.mte.common.Const;
import com.dunzung.mte.migration.ZlReadProducer;
import com.dunzung.mte.service.MonitorService;
import com.google.gson.Gson;
import com.dunzung.mte.common.EsClient;
import com.dunzung.mte.migration.ZlPendProducer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MteMain {

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
            Const.TBL.TBL_PEND_COUNT = Integer.parseInt(pendCount);
            String readCount = args[6];
            Const.TBL.TBL_READ_COUNT = Integer.parseInt(readCount);
            if (Const.TBL.TBL_PEND_COUNT <= 0 || Const.TBL.TBL_READ_COUNT <= 0) {
                System.out.println("err::TBL_PEND_COUNT::" + pendCount + "::TBL_READ_COUNT::" + readCount);
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        Class.forName("com.mysql.jdbc.Driver");
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