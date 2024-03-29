package com.atguigu.tms.publisher.controller;

import com.atguigu.tms.publisher.beans.CityAmountBean;
import com.atguigu.tms.publisher.service.CityStatsService;
import com.atguigu.tms.publisher.utils.DataFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class CityStatsController {
    @Autowired
    private CityStatsService cityStatsService;

    @RequestMapping("/cityAmount")

    public String getCityAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DataFormatUtil.now();
        }
        List<CityAmountBean> cityAmount = cityStatsService.getCityAmount(date);

        StringBuilder jsonB = new StringBuilder("{" +
                "\"status\": 0," +
                "\"data\": {" +
                "\"categories\": [");
        for (int i = 0; i < cityAmount.size(); i++) {
            CityAmountBean bean = cityAmount.get(i);
            jsonB.append('"'+bean.getCity_name()+'"');

            if(i<cityAmount.size()-1){
                jsonB.append(",");
            }

        }

        jsonB.append("]," +
                " \"series\": [" +
                " {\n" +
                "\"name\": \"地区\"," +
                "\"data\": [");

        for (int i = 0; i < cityAmount.size(); i++) {
            CityAmountBean bean = cityAmount.get(i);
            jsonB.append(bean.getOrder_amount().longValue());

            if(i<cityAmount.size()-1){
                jsonB.append(",");
            }
        }



        jsonB.append("]" +
                "}" +
                "]" +
                "}" +
                "}");
        return jsonB.toString();
    }
}
