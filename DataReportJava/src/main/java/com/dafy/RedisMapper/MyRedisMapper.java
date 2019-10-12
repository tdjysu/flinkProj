package com.dafy.RedisMapper;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisMapper implements RedisMapper<Tuple3<String, String, String>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.LPUSH);
    }
    //表示从接收的数据中获取需要操作的Redis Key
    @Override
    public String getKeyFromData(Tuple3<String, String, String> data){
        return data.f1;
    }
    //     表示从接收的数据中获取需要的Redis value
    @Override
    public String getValueFromData(Tuple3<String, String, String> data){
        return data.f2;
    }
}
