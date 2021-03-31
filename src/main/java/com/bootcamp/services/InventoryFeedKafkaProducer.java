package com.bootcamp.services;

import com.bootcamp.config.KafkaConsumerConfig;
import com.bootcamp.config.KafkaConsumerConfigurationProperties;
import com.bootcamp.config.KafkaProducerConfig;
import com.bootcamp.dto.InventoryKafkaPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.time.Duration;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
@Component
public class InventoryFeedKafkaProducer<K,V,R> extends CommonKafKaProducerService{
  private static  final Logger log = LoggerFactory.getLogger(CommonKafKaProducerService.class);
  @Autowired
  private KafkaConsumerConfig consumerConfig;

  @Autowired
  private KafkaProducerConfig producerConfig;
  @Autowired
  protected Environment env;
  @Value("${kafka.consumer.commit.interval:5s}")
  private Duration commitInterval;
  @Value("${kafka.consumer.retry.limit:0}")
  private int retryLimit;
  @Value("${ypfp.kafka.topic-name-delimiter:-}")
  private String topicNameDelimiter;
  private KafkaSender<K, V> dlqSender;
  private KafkaSender<K, V> sender;
  @Autowired
  private ObjectMapper objectMapper;
  private SimpleDateFormat dateFormat;
  private KafkaConsumerConfigurationProperties config;
  @Value("${ypfp.customer-name}")
  private String customerName;
  @Value("${streamer.inventory-update-topic}")
  private String consumerTopicName;

  @Value(("${streamer.inventory-target-topic}"))
  private String employeeTargetTopicName;


  @PostConstruct
  protected void init(){
    SenderOptions<K, V> senderOptions = SenderOptions.create(producerConfig.getProducer());
    dlqSender = KafkaSender.create(senderOptions);
    sender = KafkaSender.create(senderOptions);
  }
  public void postPayloadToTopic(InventoryKafkaPayload inventoryKafkaPayload){
    Mono<SenderRecord<K, V, R>> recordsToSend =
        (Mono<SenderRecord<K, V, R>>) Mono.just((SenderRecord<K, V, R>) SenderRecord.create(
        new ProducerRecord<K, V>(inventoryKafkaPayload.getTopic(), null,
            null,(K)inventoryKafkaPayload.getKey(), (V) inventoryKafkaPayload.getValue(),
            null), inventoryKafkaPayload.getKey()));
    sender.send(recordsToSend).doOnError(e->log.error("Error")).subscribe();
  }
}
