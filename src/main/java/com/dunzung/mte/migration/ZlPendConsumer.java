package com.dunzung.mte.migration;

import com.dunzung.mte.common.Const;
import com.dunzung.mte.common.EsClient;
import com.dunzung.mte.entity.HistPendingEntity;
import com.google.gson.Gson;

import java.util.List;

/**
 * 增量历史已办
 * <p>
 * Created by Wooola on 2019/4/30.
 */
public class ZlPendConsumer implements Runnable {

    private String threadName;

    private List<HistPendingEntity> lst;

    public ZlPendConsumer(List<HistPendingEntity> lst) {
        this.lst = lst;
    }

    @Override
    public void run() {
        threadName = Thread.currentThread().getName();
        System.out.println(threadName + "::启动...");
        try {
            System.out.println("threadName::" + threadName + "::接收::" + lst.size());
            if (lst.isEmpty()) {
                return;
            }
            lst.forEach(v -> {
                try {
                    String json = new Gson().toJson(v);
                    EsClient.addDataInJSON(json, Const.ES.HistPendDB_Index, Const.ES.HistPendDB_type, v.getPendingId(), null);
                    Const.COUNTER.LD_P.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("err::PendingId::" + v.getPendingId());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}