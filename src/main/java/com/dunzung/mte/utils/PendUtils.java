package com.dunzung.mte.utils;

import com.dunzung.mte.entity.HistPendingEntity;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Wooola on 2019/4/30.
 */
public class PendUtils {
    public static HistPendingEntity getHistPendingEntity(ResultSet rs) {
        HistPendingEntity histPendingEntity = new HistPendingEntity();
        try {
            String createDate = rs.getString("createDate");
            histPendingEntity.setCreateDate(createDate);

            String lastUpdateDate = rs.getString("lastUpdateDate");
            histPendingEntity.setLastUpdateDate(lastUpdateDate);

            String pendingCode = rs.getString("pendingCode");
            histPendingEntity.setPendingCode(pendingCode);

            String pendingNote = rs.getString("pendingNote");
            histPendingEntity.setPendingNote(pendingNote);

            String pendingUserId = rs.getString("pendingUserID");
            histPendingEntity.setPendingUserID(pendingUserId);

            String pendingTitle = rs.getString("pendingTitle");
            histPendingEntity.setPendingTitle(pendingTitle);

            String pendingCityCode = rs.getString("pendingCityCode");
            histPendingEntity.setPendingCityCode(pendingCityCode);

            String pendingURL = rs.getString("pendingURL");
            histPendingEntity.setPendingURL(pendingURL);

            String pendingDate = rs.getString("pendingDate");
            histPendingEntity.setPendingDate(pendingDate);

            String pendingStatus = rs.getString("pendingStatus");
            histPendingEntity.setPendingStatus(pendingStatus);

            String pendingSourceUserID = rs.getString("pendingSourceUserID");
            histPendingEntity.setPendingSourceUserID(pendingSourceUserID);

            String id = rs.getString("_id");
            histPendingEntity.setPendingId(id);

            String pendingSource = rs.getString("pendingSource");
            histPendingEntity.setPendingSource(pendingSource);

            String pendingLevel = rs.getString("pendingLevel");
            if (StringUtils.isNotEmpty(pendingLevel)) {
                histPendingEntity.setPendingLevel(Integer.parseInt(pendingLevel.trim()));
            }

            String pendingTitlePinyin = rs.getString("pendingTitlePinyin");
            histPendingEntity.setPendingTitlePinyin(pendingTitlePinyin);

            String pendingType = rs.getString("pendingType");
            histPendingEntity.setPendingType(pendingType);

            String pendingSourcePinyin = rs.getString("pendingSourcePinyin");
            histPendingEntity.setPendingSourcePinyin(pendingSourcePinyin);

            String collect = rs.getString("collect");
            if (StringUtils.isNotEmpty(collect)) {
                histPendingEntity.setCollect(Integer.parseInt(collect.trim()));
            }
        } catch (SQLException e) {
            System.out.println("getHistPendingEntity::_id::" + histPendingEntity.getPendingId());
            e.printStackTrace();
        }
        return histPendingEntity;
    }

}
