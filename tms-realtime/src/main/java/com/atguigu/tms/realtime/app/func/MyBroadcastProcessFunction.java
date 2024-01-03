package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.TmsConfigDimBean;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;


// 自定义类 完成主流和广播流的处理
public class MyBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor;

    private Map<String, TmsConfigDimBean> configMap = new HashMap<>();

    private String username;
    private String password;

    public MyBroadcastProcessFunction(MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor, String[] args) {
        this.mapStateDescriptor = mapStateDescriptor;
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        this.username = parameterTool.get("mysql-username", "root");
        this.password = parameterTool.get("mysql-password", "000000");

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 将配置表中的数据进行预加载-JDBC

        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://hadoop102:3306/tms_config?useSSL=false&useUnicode=true" +
                "&user=" + username + "&password=" + password +
                "&charset=utf8&TimeZone=Asia/Shanghai";
        Connection conn = DriverManager.getConnection(url);

        PreparedStatement ps = conn.prepareStatement("select * from tms_config.tms_config_dim");

        ResultSet rs = ps.executeQuery();

        ResultSetMetaData metaData = rs.getMetaData();

        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columValue = rs.getObject(i);
                jsonObj.put(columnName, columValue);
            }

            TmsConfigDimBean tmsConfigDimBean = jsonObj.toJavaObject(TmsConfigDimBean.class);
            configMap.put(tmsConfigDimBean.getSourceTable(), tmsConfigDimBean);

        }
        rs.close();
        ps.close();
        conn.close();

        super.open(parameters);
    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 获取操作的业务数据库的表名
        String table = jsonObj.getString("table");
        // 获取广播状态
        ReadOnlyBroadcastState<String, TmsConfigDimBean> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 根据操作的业务数据库的表名 到广播状态中获取对应的配置信息
        TmsConfigDimBean tmsConfigDimBean;

        if ((tmsConfigDimBean = broadcastState.get(table)) != null || (tmsConfigDimBean = configMap.get(table)) != null) {
            // 如果对应的配置信息不为空 是维度信息

            // 获取after对象，对应的是影响的业务数据表中的一条记录
            JSONObject afterJsonObj = jsonObj.getJSONObject("after");

            // 数据脱敏
            switch (table) {
                // 员工表信息脱敏
                case "employee_info":
                    String empPassword = afterJsonObj.getString("password");
                    String empRealName = afterJsonObj.getString("real_name");
                    String idCard = afterJsonObj.getString("id_card");
                    String phone = afterJsonObj.getString("phone");

                    // 脱敏
                    empPassword = DigestUtils.md5Hex(empPassword);
                    empRealName = empRealName.charAt(0) +
                            empRealName.substring(1).replaceAll(".", "\\*");
                    //知道有这个操作  idCard是随机生成的，和标准的格式不一样 所以这里注释掉
                    // idCard = idCard.matches("(^[1-9]\\d{5}(18|19|([23]\\d))\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$)|(^[1-9]\\d{5}\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{2}$)")
                    //     ? DigestUtils.md5Hex(idCard) : null;
                    phone = phone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(phone) : null;

                    afterJsonObj.put("password", empPassword);
                    afterJsonObj.put("real_name", empRealName);
                    afterJsonObj.put("id_card", idCard);
                    afterJsonObj.put("phone", phone);
                    break;
                // 快递员信息脱敏
                case "express_courier":
                    String workingPhone = afterJsonObj.getString("working_phone");
                    workingPhone = workingPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(workingPhone) : null;
                    afterJsonObj.put("working_phone", workingPhone);
                    break;
                // 卡车司机信息脱敏
                case "truck_driver":
                    String licenseNo = afterJsonObj.getString("license_no");
                    licenseNo = DigestUtils.md5Hex(licenseNo);
                    afterJsonObj.put("license_no", licenseNo);
                    break;
                // 卡车信息脱敏
                case "truck_info":
                    String truckNo = afterJsonObj.getString("truck_no");
                    String deviceGpsId = afterJsonObj.getString("device_gps_id");
                    String engineNo = afterJsonObj.getString("engine_no");

                    truckNo = DigestUtils.md5Hex(truckNo);
                    deviceGpsId = DigestUtils.md5Hex(deviceGpsId);
                    engineNo = DigestUtils.md5Hex(engineNo);

                    afterJsonObj.put("truck_no", truckNo);
                    afterJsonObj.put("device_gps_id", deviceGpsId);
                    afterJsonObj.put("engine_no", engineNo);
                    break;
                // 卡车型号信息脱敏
                case "truck_model":
                    String modelNo = afterJsonObj.getString("model_no");
                    modelNo = DigestUtils.md5Hex(modelNo);
                    afterJsonObj.put("model_no", modelNo);
                    break;
                // 用户地址信息脱敏
                case "user_address":
                    String addressPhone = afterJsonObj.getString("phone");
                    addressPhone = addressPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(addressPhone) : null;
                    afterJsonObj.put("phone", addressPhone);
                    break;
                // 用户信息脱敏
                case "user_info":
                    String passwd = afterJsonObj.getString("passwd");
                    String realName = afterJsonObj.getString("real_name");
                    String phoneNum = afterJsonObj.getString("phone_num");
                    String email = afterJsonObj.getString("email");

                    // 脱敏
                    passwd = DigestUtils.md5Hex(passwd);
                    if (StringUtils.isNotEmpty(realName)) {
                        realName = DigestUtils.md5Hex(realName);
                        afterJsonObj.put("real_name", realName);
                    }
                    phoneNum = phoneNum.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(phoneNum) : null;
                    email = email.matches("^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$")
                            ? DigestUtils.md5Hex(email) : null;

                    afterJsonObj.put("birthday", DateFormatUtil.toDate(afterJsonObj.getInteger("birthday") * 24 * 60 * 60 * 1000L));
                    afterJsonObj.put("passwd", passwd);
                    afterJsonObj.put("phone_num", phoneNum);
                    afterJsonObj.put("email", email);
                    break;
            }

            // 过滤不需要的维度属性
            String sinkColumns = tmsConfigDimBean.getSinkColumns();
            filterColum(afterJsonObj, sinkColumns);

            // 补充输出目的的表名
            String sinkTable = tmsConfigDimBean.getSinkTable();
            afterJsonObj.put("sink_table", sinkTable);

            // 补充rowKey
            String sinkPk = tmsConfigDimBean.getSinkPk();
            afterJsonObj.put("sink_pk", sinkPk);

            // 清除Redis缓存的准备工作（传递操作类型、外键字段的k-v）
            String op = jsonObj.getString("op");
            if ("u".equals(op)) {
                afterJsonObj.put("op", op);

                // 从配置表中获取当前维度表关联的外键名
                String foreignKeys = tmsConfigDimBean.getForeignKeys();
                // 定义个json对象，用于存储当前维度表对应的外键名以及值
                JSONObject foreignjsonObj = new JSONObject();
                if (StringUtils.isNotEmpty(foreignKeys)) {
                    String[] foreignNameArr = foreignKeys.split(",");
                    for (String foreignName : foreignNameArr) {
                        // 获取修改前的数据
                        JSONObject before = jsonObj.getJSONObject("before");
                        String foreignKeyBefore = before.getString(foreignName);
                        String foreignKeyAfter = afterJsonObj.getString(foreignName);

                        if (!foreignKeyBefore.equals(foreignKeyAfter)) {
                            // 如果修改的是外键
                            foreignjsonObj.put(foreignName, foreignKeyBefore);
                        }else {
                            foreignjsonObj.put(foreignName, foreignKeyBefore);

                        }

                    }

                }
                afterJsonObj.put("foreign_key", foreignjsonObj);
            }

            // 将维度数据传递
            out.collect(afterJsonObj);
        }

    }

    private void filterColum(JSONObject afterJsonObj, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fieldArr);
        Set<Map.Entry<String, Object>> entrySet = afterJsonObj.entrySet();
        entrySet.removeIf(entry -> !fieldList.contains(entry.getKey()));

    }

    @Override
    public void processBroadcastElement(String jsonStr, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);

        // 获取广播状态
        BroadcastState<String, TmsConfigDimBean> broadcastState = ctx.getBroadcastState(mapStateDescriptor);


        // 获取对配置表的操作类型
        String op = jsonObj.getString("op");
        if ("d".equals(op)) {
            String sourceTable = jsonObj.getJSONObject("before").getString("source_table");
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            TmsConfigDimBean configDimBean = jsonObj.getObject("after", TmsConfigDimBean.class);
            String sourceTable = configDimBean.getSourceTable();
            broadcastState.put(sourceTable, configDimBean);
            configMap.put(sourceTable, configDimBean);
        }
    }
}
