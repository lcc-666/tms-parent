package com.atguigu.tms.publisher.service;

import com.atguigu.tms.publisher.beans.BoundSortBean;

import java.util.List;

// 中转域统计service接口
public interface BoundStatsService {
    List<BoundSortBean> getProvinceSort(Integer date);

}
