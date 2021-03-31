package com.bootcamp.services;

import com.bootcamp.config.KafkaConsumerConfig;
import com.bootcamp.config.KafkaConsumerConfigurationProperties;
import com.bootcamp.config.KafkaProducerConfig;
import com.bootcamp.dto.InventoryKafkaPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Component
public class CommonKafKaProducerService<K,V,R>{

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


  public void postMessageToTopic(ReceiverRecord<K,V> record, String topicName, KafkaSender<K,V> sender, ReceiverOffset offset){
    Headers headers = new RecordHeaders(Arrays.stream(record.headers().toArray())
        .collect(Collectors.toList()));
    Mono<SenderRecord<K, V, R>> recordsToSend = (Mono<SenderRecord<K, V, R>>) Mono.just((SenderRecord<K, V, R>) SenderRecord.create(
        new ProducerRecord<K, V>(topicName, null, record.timestamp(),(K)record.key(),(V)record.value(), headers), record.key()));
    sender.send(recordsToSend).doOnError(e->log.error("Error")).subscribe();

  }


}