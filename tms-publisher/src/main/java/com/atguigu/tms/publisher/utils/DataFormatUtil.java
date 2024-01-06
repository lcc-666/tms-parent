package com.atguigu.tms.publisher.utils;


import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

// 日期转换工具类
public class DataFormatUtil {
    public static Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
