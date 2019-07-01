package com.example.redistaskconcumer;

import com.google.common.primitives.Longs;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.dynamic.annotation.Command;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RedisTaskConcumerApplication implements CommandLineRunner {

    private String zorderName = "zorder";

    public static void main(String[] args) {
        SpringApplication.run(RedisTaskConcumerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        RedisClient redisClient = RedisClient.create("redis://localhost:6379");
        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        RedisCommands<String, String> commands = redisConnection.sync();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();

        String queueName = "delay.q1";

        while (true) {
            ScoredValue<String> value = commands.zpopmin(zorderName);
            if (value.hasValue() == true) {
                Long score = Double.valueOf(value.getScore()).longValue();
                if (score >= System.nanoTime()) {
                    //readd task to redis
                    commands.zadd(zorderName, value.getScore(), value.getValue());
                } else {
                    //public to rabbitmq
                    channel.basicPublish("", queueName, null, Longs.toByteArray(score));
                }
            } else {
                Thread.sleep(1L);
            }
        }
    }
}
