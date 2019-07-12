package com.unicom.portal.datamigr.queue.zengl;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.entity.HistReadingEntity;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * 增量历史已阅
 * <p>
 * Created by zhijund on 2019/4/30.
 */
public class ZlReadConsumer implements Runnable {

    private String threadName;

    List<HistReadingEntity> lst;

    public ZlReadConsumer(String threadName) {
        this.threadName = threadName;
    }

    public ZlReadConsumer(List<HistReadingEntity> lst) {
        this.lst = lst;
    }

    @Override
    public void run() {
        threadName = Thread.currentThread().getName();
        System.out.println(threadName + "::启动...");
            try {
                System.out.println("threadName::" + threadName + "::接收::" + lst.size());
                if (CollectionUtils.isEmpty(lst)) {
                    return;
                }
                lst.forEach(v -> {
                    try {
                        String json = new Gson().toJson(v);
                        EsClient.addDataInJSON(json, MigrConst.ES.HistReadDB_Index, MigrConst.ES.HistReadDB_type, v.getReadingId(), null);
                        MigrConst.COUNTER.LD_R.incrementAndGet();
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