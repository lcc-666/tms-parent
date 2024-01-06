package com.atguigu.tms.publisher.service;

import java.math.BigDecimal;

//交易域统计service接口
public interface TradeStatsService {
    BigDecimal getOrderAmount(Integer date);
}
