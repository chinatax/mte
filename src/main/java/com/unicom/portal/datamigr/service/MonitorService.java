package com.unicom.portal.datamigr.service;

import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by duanzj on 2019/3/20.
 */
public class MonitorService {

    private long hisCount = 0;

    private long ldPrevPendCount = 0;

    private long ldPrevReadCount = 0;

    private long per_min_hisCount = 0;

    private long start = 0;

    public void monitor() {
        new Thread(() -> {
            while (true) {
                long unUpdateTotal = EsClient.hitTotal(MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type);
                if (hisCount == 0 || per_min_hisCount == 0) {
                    hisCount = unUpdateTotal;
                    per_min_hisCount = unUpdateTotal;
                    start = System.currentTimeMillis();
                } else {
                    StringBuilder sb = new StringBuilder();
                    sb.append("监控::已处理::" + MigrConst.PEND_COUNTER.get())
                            .append("::unUpdateTotal::" + unUpdateTotal)
                            .append("::tps::" + (hisCount - unUpdateTotal));

                    hisCount = unUpdateTotal;

                    long end = System.currentTimeMillis();
                    if ((end - start) / 1000 >= 60) {
                        start = end;
                        sb.append("::每分钟TPS::" + (per_min_hisCount - unUpdateTotal) + "条");
                        per_min_hisCount = unUpdateTotal;
                    }
                    System.out.println(sb.toString());

                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void monitorToES() {
        new Thread(() -> {
            while (true) {
                StringBuilder sb = new StringBuilder();
                sb.append("已办表数::").append(MigrConst.TBL.TBL_PEND_COUNT)
                        .append("::已办总数::").append(MigrConst.COUNTER.LD_P_TOTAL)
                        .append("::已办入库总数::").append(MigrConst.COUNTER.LD_P);
                sb.append("~~~~已阅表数::").append(MigrConst.TBL.TBL_READ_COUNT);
                sb.append("::已阅总数::").append(MigrConst.COUNTER.LD_R_TOTAL)
                        .append("::已阅入库总数::").append(MigrConst.COUNTER.LD_R);

                if (ldPrevPendCount == 0 && ldPrevReadCount == 0) {
                    ldPrevPendCount = MigrConst.COUNTER.LD_P.get();
                    ldPrevReadCount = MigrConst.COUNTER.LD_R.get();
                    start = System.currentTimeMillis();
                } else {
                    long end = System.currentTimeMillis();
                    if ((end - start) / 1000 >= 60) {
                        start = end;
                        sb.append("\n#########################################\n");
                        sb.append("已办每分钟TPS::" + (MigrConst.COUNTER.LD_P.get() - ldPrevPendCount) + "条");
                        sb.append("::已阅每分钟TPS::" + (MigrConst.COUNTER.LD_R.get() - ldPrevReadCount) + "条");
                        ldPrevPendCount = MigrConst.COUNTER.LD_P.get();
                        ldPrevReadCount = MigrConst.COUNTER.LD_R.get();
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
