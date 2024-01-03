package com.atguigu.tms.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtil {
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1000);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000L);
        poolConfig.setTestOnBorrow(true);
        jedisPool=new JedisPool(poolConfig,"hadoop102",6379,10000);
    }

    public static Jedis getJedis(){
        System.out.println("创建Jedis客户端");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }
}
