package com.four5prings.constants;

/**
 * @ClassName GmallConstants
 * @Description
 * @Author Four5prings
 * @Date 2022/6/23 15:04
 * @Version 1.0
 */
public class GmallConstants {
    //启动数据主题
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";

    //订单主题
    public static final String KAFKA_TOPIC_ORDER = "GMALL_ORDER";

    //事件主题
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";

    //预警需求索引名
    public static final String ES_ALERT_INDEXNAME = "gmall_coupon_alert";

    //订单详情主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";

    //用户主题
    public static final String KAFKA_TOPIC_USER = "TOPIC_USER_INFO";

    //灵活分析索引名
    public static final String ES_DETAIL_INDEXNAME = "gmall2021_sale_detail";

    //灵活分析索引别名
    public static final String ES_QUERY_INDEXNAME = "gmall2021_sale_detail-query";


}
