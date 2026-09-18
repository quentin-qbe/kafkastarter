package com.qbe.kafkastarter.config;

import com.qbe.kafkastarter.constants.TopicsConstants;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicCreationConfig {

    @Bean
    public NewTopic productTopic() {
        return createTopic(TopicsConstants.TOPIC_PRODUCT);
    }

    @Bean
    public NewTopic stockTopic() {
        return createTopic(TopicsConstants.TOPIC_STOCK);
    }

    @Bean
    public NewTopic marketingTopic() {
        return createTopic(TopicsConstants.TOPIC_MARKETING);
    }

    @Bean
    public NewTopic priceTopic() {
        return createTopic(TopicsConstants.TOPIC_PRICE);
    }

    @Bean
    public NewTopic supplierTopic() {
        return createTopic(TopicsConstants.TOPIC_SUPPLIER);
    }

    @Bean
    public NewTopic masterProductTopic() {
        return createTopic(TopicsConstants.TOPIC_MASTER_PRODUCT);
    }

    @Bean
    public NewTopic stockAlertTopic() {
        return createTopic(TopicsConstants.TOPIC_STOCK_ALERT);
    }

    @Bean
    public NewTopic stockAlertActionsTopic() {
        return createTopic(TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS);
    }

    @Bean
    public NewTopic stockAlertActionsDltTopic() {
        return createTopic(TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS_DEAD_LETTER_QUEUE);
    }

    private NewTopic createTopic(String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
