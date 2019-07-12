package com.unicom.portal.datamigr.queue.sunsz;

import com.google.gson.Gson;
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
public class SunszPendlProducer implements Runnable {

    private Connection conn;

    public SunszPendlProducer(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void run() {
        try {
            SearchResponse scrollResponse = EsClient.client.prepareSearch(MigrConst.ES.HistPendDB_Index)
                    .setTypes(MigrConst.ES.HistPendDB_type)
                    //.setQuery(QueryBuilders.wildcardQuery("pendingURL.keyword", MigrConst.PEND_URL_0, MigrConst.PEND_URL_1, MigrConst.PEND_URL_2))
                    .setQuery(QueryBuilders.matchPhraseQuery("pendingUserID.keyword", "gd-sansz"))
                    .setSearchType(SearchType.DEFAULT).setSize(1000).setScroll(TimeValue.timeValueMinutes(1))
                    .execute().actionGet();
            //第一次不返回数据
            SearchHits hits = scrollResponse.getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> m = hit.getSourceAsMap();
                Object pendingUserID = m.get("pendingUserID");
                if ("gd-sansz".equals(pendingUserID)){
                    try {
                        Object pendingId = m.get("pendingId");
                        m.put("pendingUserID", "gd-sunsz");
                        String json = new Gson().toJson(m);
                        EsClient.addDataInJSON(json, MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, pendingId +"", null);
                    } catch (Exception e) {
                        e.printStackTrace();
                       // System.out.println("err::pendingId:" + pendingId + "&pendingUserId=" + pendingUserId + "::json::" + json);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("SunszPendlProducer退出线程");
        }
    }
}
