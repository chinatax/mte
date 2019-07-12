package com.unicom.portal.datamigr.queue.lingdao;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.entity.HistPendingEntity;
import com.unicom.portal.datamigr.entity.HistReadingEntity;
import com.unicom.portal.datamigr.queue.PendQueue;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 领导历史待办
 * <p>
 * Created by zhijund on 2019/4/30.
 */
public class LdReadConsumer implements Runnable {

    private String threadName;

    List<HistReadingEntity> lst;

    public LdReadConsumer(String threadName) {
        this.threadName = threadName;
    }

    public LdReadConsumer(List<HistReadingEntity> lst) {
        this.lst = lst;
    }

    @Override
    public void run() {
        threadName = Thread.currentThread().getName();
        System.out.println(threadName + "::启动...");
        try {
            if (CollectionUtils.isEmpty(lst)) {
                return;
            }
            System.out.println("threadName::" + threadName + "::接收::" + lst.size());
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