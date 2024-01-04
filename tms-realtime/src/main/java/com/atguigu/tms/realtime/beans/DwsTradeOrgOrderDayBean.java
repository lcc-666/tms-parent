package com.atguigu.tms.realtime.beans;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 交易域货物类型下单聚合统计实体类
 */
@Data
@Builder
public class DwsTradeOrgOrderDayBean {

    // 日期
    String curDate;

    // 机构ID
    String orgId;

    // 机构名称
    String orgName;

    // 城市ID
    String cityId;

    // 城市名称
    String cityName;

    // 发货人区县ID
    @TransientSink
    String senderDistrictId;

    // 下单金额
    BigDecimal orderAmountBase;

    // 下单次数
    Long orderCountBase;

    // 时间戳
    Long ts;
}