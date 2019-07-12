package com.dunzung.mte.utils;

import com.dunzung.mte.entity.HistReadingEntity;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zhijund on 2019/4/30.
 */
public class ReadUtils {

    public static HistReadingEntity getHistReadingEntity(ResultSet rs) {
        HistReadingEntity histReadingEntity = new HistReadingEntity();
        try {
            String createDate = rs.getString("createDate");
            histReadingEntity.setCreateDate(createDate);

            String lastUpdateDate = rs.getString("lastUpdateDate");
            histReadingEntity.setLastUpdateDate(lastUpdateDate);

            String readingCode = rs.getString("readingCode");
            histReadingEntity.setReadingCode(readingCode);

            String readingNote = rs.getString("readingNote");
            histReadingEntity.setReadingNote(readingNote);

            String readingDate = rs.getString("readingDate");
            histReadingEntity.setReadingDate(readingDate);

            String readingStatus = rs.getString("readingStatus");
            histReadingEntity.setReadingStatus(readingStatus);

            String readingTitle = rs.getString("readingTitle");
            histReadingEntity.setReadingTitle(readingTitle);

            String readingURL = rs.getString("readingURL");
            histReadingEntity.setReadingURL(readingURL);

            String readingUserId = rs.getString("readingUserId");
            histReadingEntity.setReadingUserId(readingUserId);

            String readingSourceUserID = rs.getString("readingSourceUserID");
            histReadingEntity.setReadingSourceUserID(readingSourceUserID);

            String id = rs.getString("_id");
            histReadingEntity.setReadingId(id);

            String readingSource = rs.getString("readingSource");
            histReadingEntity.setReadingSource(readingSource);

            String readingType = rs.getString("readingType");
            histReadingEntity.setReadingType(readingType);

            String collect = rs.getString("collect");
            if (StringUtils.isNotEmpty(collect)) {
                histReadingEntity.setCollect(Integer.parseInt(collect.trim()));
            }

            String eipStatus = rs.getString("eipStatus");
            histReadingEntity.setEipStatus(eipStatus);

            String readingTitlePinyin = rs.getString("readingTitlePinyin");
            histReadingEntity.setReadingTitlePinyin(readingTitlePinyin);

            String readingSourcePinyin = rs.getString("readingSourcePinyin");
            histReadingEntity.setReadingSourcePinyin(readingSourcePinyin);

        } catch (SQLException e) {
            System.out.println("getHistReadingEntity::_id::" + histReadingEntity.getReadingId());
            e.printStackTrace();
        }
        return histReadingEntity;
    }

}
