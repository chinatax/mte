package com.unicom.portal.datamigr.queue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by duanzj on 2019/5/5.
 */
public class MigrExecutor {

    private static int THREADS = 21;

    public static ExecutorService POR = Executors.newFixedThreadPool(THREADS);

    public static ExecutorService ROR = Executors.newFixedThreadPool(THREADS);

}
