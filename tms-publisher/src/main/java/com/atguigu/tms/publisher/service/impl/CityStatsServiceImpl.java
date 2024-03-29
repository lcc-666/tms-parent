package com.atguigu.tms.publisher.service.impl;

import com.atguigu.tms.publisher.beans.CityAmountBean;
import com.atguigu.tms.publisher.mapper.CityStatsMapper;
import com.atguigu.tms.publisher.service.CityStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CityStatsServiceImpl implements CityStatsService {
    @Autowired
    private CityStatsMapper  cityStatsMapper;


    public List<CityAmountBean> getCityAmount(Integer date) {
        return cityStatsMapper.selectCityAmount(date);
    }
}
