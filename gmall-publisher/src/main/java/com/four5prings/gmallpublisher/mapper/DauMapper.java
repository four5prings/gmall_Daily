package com.four5prings.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    public Integer selectDauTotal(String date);

    /**
     * select LOGHOUR lh, count(*) ct from GMALL2022_DAU where  LOGDATE=#{date} group by LOGHOUR
     * 这里的sql结果数据是
     * +-----+------+
     * | LH  |  CT  |
     * +-----+------+
     * | 08  | 192  |
     * | 09  | 528  |
     * +-----+------+
     * 分析返回的数据类型应该是什么样子的，这个结果就是我们这个方法要返回的数据类型，用什么存储
     * 首先一定是k-v的，小时-人数
     *
     * @param date
     * @return
     */
    public List<Map> selectDauTotalHourMap(String date);
}
