package com.atguigu.tms.realtime.app.func;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


 //自定义触发器 每10s触发一次窗口计算
public class MyTriggerFunction<T>  extends Trigger<T, TimeWindow> {

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueStateDescriptor<Boolean> valueStateDescriptor
                = new ValueStateDescriptor<Boolean>("isFirstState",Boolean.class);
        ValueState<Boolean> isFirstState = ctx.getPartitionedState(valueStateDescriptor);
        Boolean isFirst = isFirstState.value();
        if(isFirst == null){
            //如果是窗口中的第一个元素
            //将状态中的值进行更新
            isFirstState.update(true);
            //注册定时器  当前事件时间向下取整后 + 10s后执行
            ctx.registerEventTimeTimer(timestamp -timestamp%10000L  + 2000L);
        }else if(isFirst){
            isFirstState.update(false);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    //time 表示事件时间触发器 触发时间
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        long end = window.getEnd();
        if(time < end){
            if(time + 2000L < end){
                ctx.registerEventTimeTimer(time + 2000L);
            }
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}

