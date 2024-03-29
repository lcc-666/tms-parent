package com.atguigu.tms.publisher.controller;

import com.atguigu.tms.publisher.beans.BoundSortBean;
import com.atguigu.tms.publisher.service.BoundStatsService;
import com.atguigu.tms.publisher.utils.DataFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class BoundStatsController {

    @Autowired
    private BoundStatsService boundStatsService;

    @RequestMapping("/provinceSort")
    public String getProvinceSort(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DataFormatUtil.now();
        }
        List<BoundSortBean> provinceSortList = boundStatsService.getProvinceSort(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceSortList.size(); i++) {
            BoundSortBean sortBean = provinceSortList.get(i);
            jsonB.append("{\"name\": \""+sortBean.getProvince_name()+"\",\"value\": "+sortBean.getSort_count()+"}");
            if(i < provinceSortList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("], \"valueName\": \"订单量\"}}");
        return jsonB.toString();

    }
}
