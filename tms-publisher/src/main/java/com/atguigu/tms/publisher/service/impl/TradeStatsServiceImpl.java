package com.atguigu.tms.publisher.service.impl;

import com.atguigu.tms.publisher.mapper.TradeStatsMapper;
import com.atguigu.tms.publisher.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

// 交易域统计接口实现类

@Service
public class TradeStatsServiceImpl implements TradeStatsService {

    @Autowired
    private TradeStatsMapper tradeStatsMapper;

    @Override
    public BigDecimal getOrderAmount(Integer date) {
        return tradeStatsMapper.selectOrderAmount(date);
    }
}
