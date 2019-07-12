package com.unicom.portal.datamigr.common;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by duanzj on 2019/4/25.
 */
public class IdsUtils {

    public static String[] getIds(List<Map<String, Object>> list) {
        List<Map<String, Object>> newList = new ArrayList<>();
        for (Map<String, Object> m : list) {
            String pid = m.get("pendingId") + "";
            if (!Cache.IDS.containsKey(pid)) {
                newList.add(m);
            }
        }
        int i = 0;
        String[] ids = new String[newList.size()];
        for (Map<String, Object> m : newList) {
            String pid = m.get("pendingId") + "";
            ids[i] = pid;
            i++;
        }
        return ids;
    }

    /**
     * 创建 id 的in语句 如果ids为空则返回空字符串
     *
     * @param ids
     * @param fieldName(比较的属性)
     * @return
     */
    public static String getSqlForIn(String[] ids, String fieldName) {
        if (ids.length <= 1000) {
            String idStr = createIds4Hql(ids);
            StringBuffer hqlBuffer = new StringBuffer("");
            if (StringUtils.isNotEmpty(idStr)) {
                hqlBuffer.append(" and " + fieldName + " in ").append(idStr);
            }
            return hqlBuffer.toString();
        } else {
            StringBuffer hqlBuffer = new StringBuffer("");
            hqlBuffer.append(" and ( ");
            int num = ids.length / 1000;
            int remain = ids.length % 1000;
            for (int i = 0; i <= num; i++) {
                String[] newIds = null;
                if (i != num) {
                    newIds = Arrays.copyOfRange(ids, i * 1000, (i + 1) * 1000);
                } else {
                    newIds = Arrays.copyOfRange(ids, i * 1000, i * 1000 + remain);
                }
                String idStr = createIds4Hql(newIds);
                if (StringUtils.isNotEmpty(idStr)) {
                    if (i == 0) {
                        hqlBuffer.append(fieldName + " in ").append(idStr);
                    } else {
                        hqlBuffer.append(" or " + fieldName + " in ").append(idStr);
                    }
                }
            }
            hqlBuffer.append(")");
            return hqlBuffer.toString();
        }
    }

    /**
     * 创建ID集合字符串，供HQL使用
     *
     * @param ids
     * @return
     */
    public static String createIds4Hql(String[] ids) {
        StringBuffer idBuffer = new StringBuffer("");
        String idsHql = null;
        StringBuffer hqlBuffer = new StringBuffer("");
        if (ids != null && ids.length > 0) {
            for (String id : ids) {
                idBuffer.append("'" + id + "',");
            }
            idsHql = StringUtils.substringBeforeLast(idBuffer.toString(), ",");
            hqlBuffer.append(" (").append(idsHql).append(")");
        }
        return hqlBuffer.toString();
    }

    public static void main(String[] args) {
        String[] ids = new String[2000];
        int i = 0;
        for (; i < ids.length; ) {
            ids[i] = i + "";
            ++i;
        }
        String idsIn = getSqlForIn(ids, "pendingId");
        System.out.println(idsIn);
    }

}
