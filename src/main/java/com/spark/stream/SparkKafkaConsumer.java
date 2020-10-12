package com.spark.stream;

import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class SparkKafkaConsumer {


    public static void main(String args[]){
try{


        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("StreamingE").setMaster("local[1]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "localhost:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "test");


    Collection<String> topics = Arrays.asList("test");

    final JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

    JavaPairDStream<String,String> stream1 = stream.mapToPair(
            new PairFunction<ConsumerRecord<String, String>, String, String>() {
                @Override
                public Tuple2<String, String> call(ConsumerRecord<String, String> record) {

                    System.out.println(record);
                    return new Tuple2<>(record.key(), record.value());
                }
            });



    stream1.map(new Function<Tuple2<String, String>, Object>() {


        @Override
        public Object call(Tuple2<String, String> stringStringTuple2) throws Exception {
            return null;
        }
    });
    stream1.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
        @Override
        public void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
            stringStringJavaPairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
                @Override
                public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                    System.out.println("key & Values "+stringStringTuple2._1 +"  :  "+ stringStringTuple2._2);
                }
            });
        }
    });

// Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();

    }catch (Exception e){
    e.printStackTrace();
    }

    }
}
