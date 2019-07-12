package com.dunzung.mte.migration;

import com.dunzung.mte.common.Const;
import com.dunzung.mte.common.EsClient;
import com.dunzung.mte.entity.HistReadingEntity;
import com.google.gson.Gson;

import java.util.List;

/**
 * 增量历史已阅
 * <p>
 * Created by Wooola on 2019/4/30.
 */
public class ZlReadConsumer implements Runnable {

    private String threadName;

    private List<HistReadingEntity> lst;

    public ZlReadConsumer(List<HistReadingEntity> lst) {
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
                        EsClient.addDataInJSON(json, Const.ES.HistReadDB_Index, Const.ES.HistReadDB_type, v.getReadingId(), null);
                        Const.COUNTER.LD_R.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("err::ReadingId::" + v.getReadingId());
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
}