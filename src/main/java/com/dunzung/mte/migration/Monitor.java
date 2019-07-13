package com.dunzung.mte.migration;

import com.dunzung.mte.common.Const;

/**
 * Created by Wooola on 2019/3/20.
 */
public class Monitor {

    private long ldPrevPendCount = 0;

    private long ldPrevReadCount = 0;

    private long start = 0;

    public void monitorToES() {
        new Thread(() -> {
            while (true) {
                StringBuilder sb = new StringBuilder();
                sb.append("已办表数::").append(Const.TBL.TBL_PEND_COUNT)
                        .append("::已办总数::").append(Const.COUNTER.LD_P_TOTAL)
                        .append("::已办入库总数::").append(Const.COUNTER.LD_P);
                sb.append("~~~~已阅表数::").append(Const.TBL.TBL_READ_COUNT);
                sb.append("::已阅总数::").append(Const.COUNTER.LD_R_TOTAL)
                        .append("::已阅入库总数::").append(Const.COUNTER.LD_R);

                if (ldPrevPendCount == 0 && ldPrevReadCount == 0) {
                    ldPrevPendCount = Const.COUNTER.LD_P.get();
                    ldPrevReadCount = Const.COUNTER.LD_R.get();
                    start = System.currentTimeMillis();
                } else {
                    long end = System.currentTimeMillis();
                    if ((end - start) / 1000 >= 60) {
                        start = end;
                        sb.append("\n#########################################\n");
                        sb.append("已办每分钟TPS::" + (Const.COUNTER.LD_P.get() - ldPrevPendCount) + "条");
                        sb.append("::已阅每分钟TPS::" + (Const.COUNTER.LD_R.get() - ldPrevReadCount) + "条");
                        ldPrevPendCount = Const.COUNTER.LD_P.get();
                        ldPrevReadCount = Const.COUNTER.LD_R.get();
                    }
                }
                System.out.println(sb.toString());
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
