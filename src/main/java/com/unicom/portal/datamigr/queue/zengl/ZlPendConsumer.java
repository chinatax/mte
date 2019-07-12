package com.unicom.portal.datamigr.queue.zengl;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.entity.HistPendingEntity;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * 增量历史已办
 * <p>
 * Created by zhijund on 2019/4/30.
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
            if (CollectionUtils.isEmpty(lst)) {
                return;
            }
            lst.forEach(v -> {
                try {
                    String json = new Gson().toJson(v);
                    EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, v.getPendingId(), null);
                    MigrConst.COUNTER.LD_P.incrementAndGet();
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