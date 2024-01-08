package com.atguigu.tms.publisher.mapper;

import com.atguigu.tms.publisher.beans.BoundSortBean;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface BoundStatsMapper {
    // 获取某天各个省份的分拣数
    @Select("select \n" +
            "    province_name,\n" +
            "    sum(sort_count) sort_count\n" +
            "from\n" +
            " (\n" +
            "   SELECT\n" +
            "    cur_date,\n" +
            "    org_id,\n" +
            "    org_name,\n" +
            "    city_id,\n" +
            "    city_name,\n" +
            "    province_id,\n" +
            "    province_name,\n" +
            "    argMaxMerge(sort_count) AS sort_count\n" +
            "FROM dws_bound_org_sort_day where toYYYYMMDD(cur_date)=#{date}\n" +
            "GROUP BY\n" +
            "    cur_date,\n" +
            "    org_id,\n" +
            "    org_name,\n" +
            "    city_id,\n" +
            "    city_name,\n" +
            "    province_id,\n" +
            "    province_name\n" +
            "\n" +
            "\n" +
            " )\n" +
            "group by province_name;")
    List<BoundSortBean> selectProvinceSort(Integer date);

}
