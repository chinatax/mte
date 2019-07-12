package com.unicom.portal.datamigr.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.util.CollectionUtils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by duanzj on 2018/11/12.
 */
public class EsClient {

    public static TransportClient client;

    public static long hitTotal(String index, String type) {
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.wildcardQuery("pendingURL.keyword", MigrConst.PEND_URL + "*"))
               // .setQuery(QueryBuilders.termsQuery("pendingURL", MigrConst.PEND_URL_0, MigrConst.PEND_URL_1, MigrConst.PEND_URL_2))
                .execute().actionGet();
        return response.getHits().totalHits;

    }

    public static List<Map<String, Object>> searchForList(String index, String type, Map<String, Object> queryMap, Integer size) {
        SearchRequestBuilder responsebuilder = client.prepareSearch(index).setTypes(type);
        QueryBuilder qb = QueryBuilders.boolQuery();
        if (!CollectionUtils.isEmpty(queryMap)) {
            for (String key : queryMap.keySet()) {
                MatchPhraseQueryBuilder mpq1 = QueryBuilders.matchPhraseQuery(key, queryMap.get(key).toString());
                ((BoolQueryBuilder) qb).must(mpq1);
            }
        }
        if (null == size) {
            size = 10000;
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(qb);

        SearchResponse myresponse = responsebuilder
                .setFrom(0)
                .setQuery(qb)
                .setSize(size)
                .setExplain(true)
                .execute()
                .actionGet();
        SearchHits hits = myresponse.getHits();
        List<Map<String, Object>> listMap = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : hits) {
            listMap.add(hit.getSourceAsMap());
        }
        return listMap;
    }

    /**
     * 数据添加，正定ID
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type) {
        IndexResponse response = client.prepareIndex(index, type).setSource(jsonObject).get();
        return response.getId();
    }

    public static String addData(JSONObject jsonObject, String index, String type, String id) {
        IndexResponse response = client.prepareIndex(index, type, id).setSource(jsonObject).get();
        return response.getId();
    }

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) {
        CreateIndexResponse indexresponse = client.admin().indices().prepareCreate(index).execute().actionGet();
        return indexresponse.isAcknowledged();
    }

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean deleteIndex(String index) {
        DeleteIndexResponse indexresponse = client.admin().indices().prepareDelete(index).execute().actionGet();
        return indexresponse.isAcknowledged();
    }

    /**
     * 高亮结果集 特殊处理
     *
     * @param searchResponse
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse) {
        List<Map<String, Object>> sourceList = new ArrayList<>();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());
            sourceList.add(searchHit.getSourceAsMap());
        }
        return sourceList;
    }

    public static String addDataInJSON(String jsonStr, String index, String type, String id, XContentType contentType) {
        if (null == contentType) {
            contentType = XContentType.JSON;
        }
        IndexResponse response = null;
        if (StringUtils.isEmpty(id)) {
            response = client.prepareIndex(index, type).setSource(jsonStr, contentType).get();
        } else {
            response = client.prepareIndex(index, type, id).setSource(jsonStr, contentType).get();
        }
        return response.getId();
    }


    //    Settings esSetting = Settings.builder()
//            .put("cluster.name", "zhmh-xnj-es")
//            .put("client.transport.sniff", true)//增加嗅探机制，找到ES集群
//            .put("thread_pool.search.size", 5)//增加线程池个数，暂时设为5
//            .build();
//    String[] nodes = "10.236.9.154:9300,10.236.9.155:9300,10.236.9.156:9300".split(",");
    public synchronized static void initClient(Map<String, String> meta) {
        try {
            String cName = meta.get("cName");
            String esNodes = meta.get("esNodes");
            Settings esSetting = Settings.builder()
                    .put("cluster.name", cName)
                    .put("client.transport.sniff", true)//增加嗅探机制，找到ES集群
                    .put("thread_pool.search.size", 5)//增加线程池个数，暂时设为5
                    .build();
            String[] nodes = esNodes.split(",");
            client = new PreBuiltTransportClient(esSetting);
            for (String node : nodes) {
                if (node.length() > 0) {
                    String[] hostPort = node.split(":");
                    client.addTransportAddress(new TransportAddress(InetAddress.getByName(hostPort[0]), Integer.parseInt(hostPort[1])));
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("ES初始化完成...");
    }


}

