package com.unicom.portal.datamigr.queue.url;

import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.common.MigrUtils;
import com.unicom.portal.datamigr.queue.PendQueue;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhijund on 2019/4/27.
 */
public class PendUrlProducer implements Runnable {

    private Connection conn;

    public PendUrlProducer(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void run() {
        try {
            System.out.println("PendUrlProducer启动...");
            while (true) {
                SearchResponse scrollResponse = EsClient.client.prepareSearch(MigrConst.ES.HistPendDB_Index)
                        .setTypes(MigrConst.ES.HistPendDB_type)
                        //.setQuery(QueryBuilders.wildcardQuery("pendingURL.keyword", MigrConst.PEND_URL_0, MigrConst.PEND_URL_1, MigrConst.PEND_URL_2))
                        .setQuery(QueryBuilders.wildcardQuery("pendingURL.keyword", MigrConst.PEND_URL + "*"))
                        .setSearchType(SearchType.DEFAULT).setSize(2000).setScroll(TimeValue.timeValueMinutes(1))
                        .execute().actionGet();
                //第一次不返回数据
                long count = scrollResponse.getHits().getTotalHits();
                for (int sum = 0; sum < count; ) {
                    if (sum >= count) {
                        break;
                    }
                    scrollResponse = EsClient.client.prepareSearchScroll(scrollResponse.getScrollId())
                            .setScroll(TimeValue.timeValueMinutes(1))
                            .execute().actionGet();
                    SearchHits hits = scrollResponse.getHits();
                    sum += hits.getHits().length;
                    System.out.println("总量::" + count + "::已经查到::" + sum);
                    List<Map<String, Object>> listMap = new ArrayList<>();
                    for (SearchHit hit : hits) {
                        Map<String, Object> m = hit.getSourceAsMap();
                        String purl = m.get("pendingURL") + "";
                        if (MigrUtils.hasTrue(purl)) {
                            listMap.add(hit.getSourceAsMap());
                        }
                    }
                    if (MigrConst.PEND_DB_ENABLE && !listMap.isEmpty()) {
                        new Thread(() -> {
                            MigrUtils.insertToDB(conn, listMap);
                        }).start();
                    }
                    System.out.println("总量::" + count + "::已经查到::" + sum + "::有效::" + listMap.size() + "条");
                    PendQueue.P_U_Q.offer(listMap);
                    Thread.sleep(3000);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("PendUrlProducer退出线程");
        }
    }
}
