package com.unicom.portal.datamigr.queue.sunsz;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.sql.Connection;
import java.util.Map;

/**
 * Created by zhijund on 2019/4/27.
 */
public class SunszReadProducer implements Runnable {

    private Connection conn;

    public SunszReadProducer(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void run() {
        try {
            SearchResponse scrollResponse = EsClient.client.prepareSearch(MigrConst.ES.HistReadDB_Index)
                    .setTypes(MigrConst.ES.HistReadDB_type)
                    //.setQuery(QueryBuilders.wildcardQuery("pendingURL.keyword", MigrConst.PEND_URL_0, MigrConst.PEND_URL_1, MigrConst.PEND_URL_2))
                    .setQuery(QueryBuilders.matchPhraseQuery("readingUserId.keyword", "gd-sansz"))
                    .setSearchType(SearchType.DEFAULT).setSize(1000).setScroll(TimeValue.timeValueMinutes(1))
                    .execute().actionGet();
            //第一次不返回数据
            SearchHits hits = scrollResponse.getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> m = hit.getSourceAsMap();
                Object readingUserID = m.get("readingUserId");
                if ("gd-sansz".equals(readingUserID)){
                    try {
                        Object readingId = m.get("readingId");
                        m.put("readingUserId", "gd-sunsz");
                        String json = new Gson().toJson(m);
                        EsClient.addDataInJSON(json, MigrConst.ES.HistReadDB_Index, MigrConst.ES.HistReadDB_type, readingId +"", null);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("SunszReadProducer退出线程");
        }
    }
}
