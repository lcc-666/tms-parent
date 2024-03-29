package com.atguigu.tms.publisher.service;

import com.atguigu.tms.publisher.beans.CityAmountBean;

import java.util.List;

public interface CityStatsService {
    List<CityAmountBean> getCityAmount(Integer date);
}
