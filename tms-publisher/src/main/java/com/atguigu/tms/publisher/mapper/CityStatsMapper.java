package com.atguigu.tms.publisher.mapper;

import com.atguigu.tms.publisher.beans.CityAmountBean;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface CityStatsMapper {
    // 获取订单金额最大的五个城市

    @Select("SELECT \n" +
            "    city_name,\n" +
            "    SUM(order_amount) AS order_amount \n" +
            "FROM (\n" +
            "    SELECT \n" +
            "        cur_date, \n" +
            "        org_id, \n" +
            "        org_name, \n" +
            "        city_id, \n" +
            "        city_name, \n" +
            "        argMaxMerge(order_amount) AS order_amount, \n" +
            "        argMaxMerge(order_count) AS order_count\n" +
            "    FROM dws_trade_org_order_day where toYYYYMMDD(cur_date)=#{date}\n" +
            "    GROUP BY \n" +
            "        cur_date, \n" +
            "        org_id, \n" +
            "        org_name, \n" +
            "        city_id, \n" +
            "        city_name\n" +
            ")GROUP BY city_name\n" +
            "ORDER BY order_amount DESC \n" +
            "LIMIT 10;")
    List<CityAmountBean> selectCityAmount(Integer date);

}
