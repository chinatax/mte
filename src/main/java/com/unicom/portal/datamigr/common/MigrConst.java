package com.unicom.portal.datamigr.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by duanzj on 2019/3/20.
 */
public final class MigrConst {

    public static final class ES {

        public static final String HistPendDB_Index = "histpenddb";

        public static final String HistPendDB_type = "histpending";

        public static final String HistReadDB_Index = "histreaddb";

        public static final String HistReadDB_type = "histreading";

    }

    public static final class TBL {

        public static int TBL_PEND_COUNT = 0;

        public static int TBL_READ_COUNT = 0;

    }

    public static final class COUNTER {

        public static AtomicLong LD_R = new AtomicLong();

        public static AtomicLong LD_R_TOTAL = new AtomicLong();

        public static AtomicLong LD_P = new AtomicLong();

        public static AtomicLong LD_P_TOTAL = new AtomicLong();

    }


    // 增量5000
    public static AtomicLong PEND_COUNTER = new AtomicLong();

    /**
     * pengdingURL=1完成
     * pengdingURL=2 完成
     */
    public static final String PEND_URL = "0";

    public static final String PEND_URL_0 = "0";

    public static final String PEND_URL_1 = "1";

    public static final String PEND_URL_2 = "2";

    public static final boolean PEND_DB_ENABLE = false;

}
