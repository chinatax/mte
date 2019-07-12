package com.unicom.portal.datamigr.service;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.entity.HistReadingEntity;
import com.unicom.portal.datamigr.common.EsClient;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Map;

/**
 * Created by duanzj on 2019/3/20.
 */
public class HisReadService {

    public void synHisRead(Map<String, String> meta) {
        System.out.println("synHisRead>>>Start");
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = meta.get("url");
            String user = meta.get("user");
            String password = meta.get("password");
            Connection con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
            }
            Statement statement = con.createStatement();
            String countSql = "SELECT COUNT(1)  cou FROM reading_temp";
            ResultSet rsCount = statement.executeQuery(countSql);
            ResultSet rs = null;
            int count = 0;
            while (rsCount.next()) {
                count = Integer.parseInt(rsCount.getString("cou"));
            }
            System.out.println("read count::" + count);
            int size = 1000;
            for (int i = 0; i < count; i += size) {
                if (i + size > count) {
                    //作用为size最后没有100条数据则剩余几条newList中就装几条
                    size = count - i;
                }
                // String sql = "select * from reading_temp limit " + i + "," + size;
                String sql = "select * from reading_temp where readingUserId='changp3' ";
                System.out.println("sql>>>" + sql);
                rs = statement.executeQuery(sql);
                String id = null;
                String readingCode = null;
                String readingTitle = null;
                String readingDate = null;
                String readingUserId = null;
                String readingURL = null;
                String readingStatus = null;
                String readingSourceUserID = null;
                String readingSource = null;
                String readingNote = null;
                String eipStatus = null;
                String readingType = null;
                String createDate = null;
                String lastUpdateDate = null;
                String readingTitlePinyin = null;
                String readingSourcePinyin = null;
                String collect = null;
                while (rs.next()) {
                    HistReadingEntity histReadingEntity = new HistReadingEntity();
                    try {
                        id = rs.getString("_id");
                        histReadingEntity.setReadingId(id);
                        readingCode = rs.getString("readingCode");
                        histReadingEntity.setReadingCode(readingCode);
                        readingTitle = rs.getString("readingTitle");
                        histReadingEntity.setReadingTitle(readingTitle);
                        readingDate = rs.getString("readingDate");
                        histReadingEntity.setReadingDate(readingDate);
                        readingUserId = rs.getString("readingUserId");
                        histReadingEntity.setReadingUserId(readingUserId);
                        readingURL = rs.getString("readingURL");
                        histReadingEntity.setReadingURL(readingURL);
                        readingStatus = rs.getString("readingStatus");
                        histReadingEntity.setReadingStatus(readingStatus);
                        readingSourceUserID = rs.getString("readingSourceUserID");
                        histReadingEntity.setReadingSourceUserID(readingSourceUserID);
                        readingSource = rs.getString("readingSource");
                        histReadingEntity.setReadingSource(readingSource);
                        readingNote = rs.getString("readingNote");
                        histReadingEntity.setReadingNote(readingNote);
                        eipStatus = rs.getString("eipStatus");
                        histReadingEntity.setEipStatus(eipStatus);
                        readingType = rs.getString("readingType");
                        histReadingEntity.setReadingType(readingType);
                        createDate = rs.getString("createDate");
                        histReadingEntity.setCreateDate(createDate);
                        lastUpdateDate = rs.getString("lastUpdateDate");
                        histReadingEntity.setLastUpdateDate(lastUpdateDate);
                        readingTitlePinyin = rs.getString("readingTitlePinyin");
                        histReadingEntity.setReadingTitlePinyin(readingTitlePinyin);
                        readingSourcePinyin = rs.getString("readingSourcePinyin");
                        histReadingEntity.setReadingSourcePinyin(readingSourcePinyin);
                        collect = rs.getString("collect");
                        if (StringUtils.isNotEmpty(collect)) {
                            histReadingEntity.setCollect(Integer.parseInt(collect));
                        }
                        EsClient.addData(JSON.parseObject(new Gson().toJson(histReadingEntity)), MigrConst.ES.HistReadDB_Index, MigrConst.ES.HistReadDB_type, histReadingEntity.getReadingId());
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("err:id::" + histReadingEntity.getReadingId());
                    }
                }
                Thread.sleep(200);
            }
            rs.close();
            rsCount.close();
            con.close();
            System.out.println("已阅数据同步ES成功！！");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
