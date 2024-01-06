package com.atguigu.tms.publisher.controller;

import com.atguigu.tms.publisher.service.TradeStatsService;
import com.atguigu.tms.publisher.utils.DataFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.crypto.Data;
import java.math.BigDecimal;

@RestController
public class TradeStatsController {
    @Autowired
    private TradeStatsService tradeStatsService;

    @RequestMapping("/orderAmount")

    public String getOrderAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DataFormatUtil.now();
        }
        BigDecimal orderAmount = tradeStatsService.getOrderAmount(date);
        return "{\"status\":0,\"data\":" + orderAmount + "}";
    }


}
