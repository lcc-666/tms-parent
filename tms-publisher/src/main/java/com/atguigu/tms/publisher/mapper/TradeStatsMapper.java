package com.atguigu.tms.publisher.mapper;


import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;


// 交易域统计mapper
public interface TradeStatsMapper {
    // 获取某天下单总金额
    @Select("select \n" +
            "    sum(order_amount)\n" +
            "from\n" +
            " (\n" +
            "   select \n" +
            "    cur_date,\n" +
            "    org_id,\n" +
            "    org_name,\n" +
            "    city_id,\n" +
            "    city_name,\n" +
            "    argMaxMerge(order_amount) as order_amount,\n" +
            "    argMaxMerge(order_count) as order_count \n" +
            "from dws_trade_org_order_day where toYYYYMMDD(cur_date)=#{date}\n" +
            "group by cur_date,\n" +
            "    org_id,\n" +
            "    org_name,\n" +
            "    city_id,\n" +
            "    city_name\n" +
            " )")
    BigDecimal selectOrderAmount(Integer date);

}
