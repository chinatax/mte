package com.unicom.portal.datamigr.service;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.unicom.portal.datamigr.common.EsClient;
import com.unicom.portal.datamigr.common.MigrConst;
import com.unicom.portal.datamigr.entity.HistPendingEntity;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Map;

/**
 * Created by duanzj on 2019/3/20.
 */
public class HisPendService {

    public void synHisPend(Map<String, String> meta) {
        System.out.println("synHisPend>>>Start");
        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            String url = meta.get("url");
            String user = meta.get("user");
            String password = meta.get("password");
            Connection con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
            }
            Statement statement = con.createStatement();
            String countSql = "SELECT COUNT(1)  cou FROM pending_temp";
            ResultSet rsCount = statement.executeQuery(countSql);
            ResultSet rs = null;
            int count = 0;
            while (rsCount.next()) {
                count = Integer.parseInt(rsCount.getString("cou"));
            }
            System.out.println("pend count>>" + count);
            int size = 1000;
            for (int i = 0; i < count; i += size) {
                if (i + size > count) {//作用为size最后没有100条数据则剩余几条newList中就装几条
                    size = count - i;
                }
                String sql = "select * from pending_temp limit " + i + "," + size;
                System.out.println("sql>>>" + sql);
                rs = statement.executeQuery(sql);
                String id = null;
                String pendingCode = null;
                String pendingTitle = null;
                String pendingDate = null;
                String pendingUserId = null;
                String pendingURL = null;
                String pendingLevel = null;
                String pendingStatus = null;
                String pendingSourceUserID = null;
                String pendingSource = null;
                String pendingNote = null;
                String pendingCityCode = null;
                String pendingType = null;
                String createDate = null;
                String lastUpdateDate = null;
                String pendingTitlePinyin = null;
                String pendingSourcePinyin = null;
                String collect = null;
                while (rs.next()) {
                    HistPendingEntity histPendingEntity = new HistPendingEntity();
                    try {
                        id = rs.getString("_id");
                        histPendingEntity.setPendingId(id);
                        pendingCode = rs.getString("pendingCode");
                        histPendingEntity.setPendingCode(pendingCode);
                        pendingTitle = rs.getString("pendingTitle");
                        histPendingEntity.setPendingTitle(pendingTitle);
                        pendingDate = rs.getString("pendingDate");
                        histPendingEntity.setPendingDate(pendingDate);
                        pendingUserId = rs.getString("pendingUserId");
                        histPendingEntity.setPendingUserID(pendingUserId);
                        pendingURL = rs.getString("pendingURL");
                        histPendingEntity.setPendingURL(pendingURL);
                        pendingLevel = rs.getString("pendingLevel");
                        if(StringUtils.isNotEmpty(pendingLevel)) {
                            histPendingEntity.setPendingLevel(Integer.parseInt(pendingLevel));
                        }
                        pendingStatus = rs.getString("pendingStatus");
                        histPendingEntity.setPendingStatus(pendingStatus);
                        pendingSourceUserID = rs.getString("pendingSourceUserID");
                        histPendingEntity.setPendingSourceUserID(pendingSourceUserID);
                        pendingSource = rs.getString("pendingSource");
                        histPendingEntity.setPendingSource(pendingSource);
                        pendingNote = rs.getString("pendingNote");
                        histPendingEntity.setPendingNote(pendingNote);
                        pendingCityCode = rs.getString("pendingCityCode");
                        histPendingEntity.setPendingCityCode(pendingCityCode);
                        pendingType = rs.getString("pendingType");
                        histPendingEntity.setPendingType(pendingType);
                        createDate = rs.getString("createDate");
                        histPendingEntity.setCreateDate(createDate);
                        lastUpdateDate = rs.getString("lastUpdateDate");
                        histPendingEntity.setLastUpdateDate(lastUpdateDate);
                        pendingTitlePinyin = rs.getString("pendingTitlePinyin");
                        histPendingEntity.setPendingTitlePinyin(pendingTitlePinyin);
                        pendingSourcePinyin = rs.getString("pendingSourcePinyin");
                        histPendingEntity.setPendingSourcePinyin(pendingSourcePinyin);
                        collect = rs.getString("collect");
                        if(StringUtils.isNotEmpty(collect)) {
                            histPendingEntity.setCollect(Integer.parseInt(collect));
                        }
                        EsClient.addData(JSON.parseObject(new Gson().toJson(histPendingEntity)), MigrConst.ES.HistPendDB_Index, MigrConst.ES.HistPendDB_type, histPendingEntity.getPendingId());
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("err:id::" + histPendingEntity.getPendingId());
                    }
                }

                Thread.sleep(200);
            }
            rs.close();
            rsCount.close();
            con.close();
            System.out.println("已办数据同步ES成功！！");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
