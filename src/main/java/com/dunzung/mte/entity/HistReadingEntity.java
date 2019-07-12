package com.dunzung.mte.entity;
import lombok.Data;

/**
 * 历史已阅对象
 */
@Data
//@Document(indexName = "hist_reading")
public class HistReadingEntity {

	private String readingId;
	private String readingCode;                       //待阅信息的编号
    private String readingTitle;                      //待阅标题
    private String readingDate;                       //待阅产生时间
    private String readingUserId;                     //待阅人UserID 
    private String readingURL;                        //待阅信息URL 
    private String readingStatus;                     //待阅状态 
    private String readingSourceUserID;               //待阅信息发布人员的帐号
    private String readingSource;                     //待阅信息来源
    private String readingNote;                       //待阅所属系统简称 
    private String eipStatus;                         //该条待阅在EIP中的状态
    private String readingType;                       //待阅类型
    private String createDate;                        //创建时间   
    private String lastUpdateDate;                    //更新时间
    private String readingTitlePinyin;                //待阅标题拼音
    private String readingSourcePinyin;               //待阅信息来源拼音
    private int collect;                              //收藏
    private String instanceID;
    private String proxyAppID;
    public static final String preStatus = "0";
    public static final String afterStatus = "1";
    public static final String deleteStatus = "d";

}
