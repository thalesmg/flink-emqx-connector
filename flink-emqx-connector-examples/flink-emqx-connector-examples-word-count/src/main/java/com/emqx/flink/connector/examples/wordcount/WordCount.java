package com.emqx.flink.connector.examples.wordcount;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.emqx.flink.connector.EMQXMessage;
import com.emqx.flink.connector.EMQXSource;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokerHost = "emqx1.emqx.net";
        int brokerPort = 1883;
        String clientid = "cid";
        String groupName = "gname";
        String topicFilter = "t/#";
        int qos = 1;
        DeserializationSchema<String> deserializer = new StringDeserializer();
        EMQXSource<String> emqx = new EMQXSource<>(brokerHost, brokerPort, clientid, groupName, topicFilter, qos,
                deserializer);
        DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqx, WatermarkStrategy.noWatermarks(), "emqx");
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = source
                .flatMap(new Keyer())
                .name("keyer")
                .keyBy(tup -> tup.f0);
        keyedStream.sum(1).print();
        env.execute();
    }

    public static class Keyer implements FlatMapFunction<EMQXMessage<String>, Tuple2<String, Integer>> {
        @Override
        public void flatMap(EMQXMessage<String> msg, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(msg.payload, 1));
        }
    }
}
