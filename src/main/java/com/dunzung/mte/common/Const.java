package com.dunzung.mte.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Wooola on 2019/3/20.
 */
public final class Const {

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

}
