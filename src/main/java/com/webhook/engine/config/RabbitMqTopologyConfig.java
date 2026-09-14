package com.webhook.engine.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class RabbitMqTopologyConfig {

    public static final String WORK_EXCHANGE = "webhook.work.exchange";
    public static final String WORK_QUEUE = "webhook.work.queue";
    public static final String WORK_ROUTING_KEY = "webhook.dispatch";

    public static final String DELAY_EXCHANGE = "webhook.delay.exchange";
    public static final String RETRY_ROUTING_KEY = "webhook.retry";

    public static final String DLQ_EXCHANGE = "webhook.dlq.exchange";
    public static final String DLQ_QUEUE = "webhook.dlq.queue";
    public static final String DLQ_ROUTING_KEY = "webhook.deadletter";

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public DirectExchange workExchange() {
        return new DirectExchange(WORK_EXCHANGE, true, false);
    }

    @Bean
    public Queue workQueue() {
        return QueueBuilder.durable(WORK_QUEUE).build();
    }

    @Bean
    public Binding workBinding(Queue workQueue, DirectExchange workExchange) {
        return BindingBuilder.bind(workQueue).to(workExchange).with(WORK_ROUTING_KEY);
    }

    // Requires rabbitmq_delayed_message_exchange plugin installed on RabbitMQ
    @Bean
    public CustomExchange delayedExchange() {
        Map<String, Object> args = Map.of("x-delayed-type", "direct");
        return new CustomExchange(DELAY_EXCHANGE, "x-delayed-message", true, false, args);
    }

    @Bean
    public Binding retryBinding(Queue workQueue, CustomExchange delayedExchange) {
        return BindingBuilder.bind(workQueue).to(delayedExchange).with(RETRY_ROUTING_KEY).noargs();
    }

    @Bean
    public DirectExchange dlqExchange() {
        return new DirectExchange(DLQ_EXCHANGE, true, false);
    }

    @Bean
    public Queue dlqQueue() {
        return QueueBuilder.durable(DLQ_QUEUE).build();
    }

    @Bean
    public Binding dlqBinding(Queue dlqQueue, DirectExchange dlqExchange) {
        return BindingBuilder.bind(dlqQueue).to(dlqExchange).with(DLQ_ROUTING_KEY);
    }
}
