package com.dunzung.mte.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Map;

/**
 * Created by Wooola on 2018/11/12.
 */
public class EsClient {

    public static TransportClient client;

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

