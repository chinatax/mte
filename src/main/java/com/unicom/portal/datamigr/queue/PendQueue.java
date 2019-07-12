package com.unicom.portal.datamigr.queue;

import com.unicom.portal.datamigr.entity.HistPendingEntity;
import com.unicom.portal.datamigr.entity.HistReadingEntity;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zhijund on 2019/4/27.
 */
public class PendQueue {

    // pend_url_update
    public static BlockingQueue<List<Map<String, Object>>> P_U_Q = new LinkedBlockingQueue<>(1000);

    public static BlockingQueue<List<HistPendingEntity>> P_Q = new LinkedBlockingQueue<>(50000);

    public static BlockingQueue<List<HistReadingEntity>> R_Q = new LinkedBlockingQueue<>(50000);

}
