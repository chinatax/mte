package com.unicom.portal.datamigr.service;

import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by duanzj on 2019/3/20.
 */
public class UpdateReadService {

    public void queryRead(Map<String, String> meta) {
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("readingId", "na120hq-356446-changp3");
        List<Map<String, Object>> list = EsClient.searchForList(MigrConst.ES.HistReadDB_Index, MigrConst.ES.HistReadDB_type, queryMap, 10000);
        list.forEach(v -> {
            System.out.println(new Gson().toJson(v));
        });
    }

    public void updateRead(Map<String, String> meta) {
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("readingId", "na120hq-356446-changp3");
        List<Map<String, Object>> list = EsClient.searchForList(MigrConst.ES.HistReadDB_Index, MigrConst.ES.HistReadDB_type, queryMap, 10000);
        list.forEach(v -> {
            System.out.println(new Gson().toJson(v));
        });
    }

}
