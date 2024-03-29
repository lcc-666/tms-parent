package com.atguigu.tms.publisher.beans;

import lombok.Data;

import java.math.BigDecimal;


@Data
public class CityAmountBean {
    String city_name;
    BigDecimal order_amount;
}
