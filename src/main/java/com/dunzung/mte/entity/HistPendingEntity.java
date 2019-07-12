package com.dunzung.mte.entity;

import lombok.Data;

@Data
public class HistPendingEntity {
    private String pendingId;
    private String pendingCode;                 //专业系统待办编号
    private String pendingTitle;                //待办标题
    private String pendingDate;                 //待办产生时间
    private String pendingUserID;               //待办人UserID      
    private String pendingURL;                  //待办信息URL 
    private String pendingStatus;               //待办状态 
    private int pendingLevel;                   //待办等级
    private String pendingCityCode;             //省份代码  
    private String pendingSourceUserID;         //待办信息上一步处理人邮件前缀  
    private String pendingSource;               //待办信息来源
    private String pendingNote;                 //待办所属系统简称
    private String pendingType;                 //待办类型
    private String createDate;                  //创建时间
    private String lastUpdateDate;              //修改时间 
    private String pendingTitlePinyin;          //待办标题拼音首字母
    private String pendingSourcePinyin;         //待办信息来源拼音首字母   
    private int collect;                        //收藏
    private String instanceID;                  //实例ID
    private String proxyAppID;                  //代理服务ID或者专业系统ID
    public static final String preStatus = "0";
    public static final String afterStatus = "1";
    public static final String deleteStatus = "d";
}