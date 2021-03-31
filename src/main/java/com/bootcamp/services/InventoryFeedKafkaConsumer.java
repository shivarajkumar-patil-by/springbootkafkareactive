package com.bootcamp.services;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import com.bootcamp.config.KafkaConsumerConfig;
import com.bootcamp.config.KafkaConsumerConfigurationProperties;
import com.bootcamp.config.KafkaProducerConfig;
import com.bootcamp.dto.InventoryKafkaPayload;
import com.bootcamp.dto.InventoryUpdateKey;
import com.bootcamp.dto.InventoryUpdatePayload;
import com.bootcamp.dto.InventoryUpdateValue;
import com.bootcamp.dto.SupplyTo;
import com.bootcamp.dto.SupplyToPayload;
import com.bootcamp.dto.SupplyType;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Component
public class InventoryFeedKafkaConsumer<K,V,R> {
  private final Logger LOG = LoggerFactory.getLogger(InventoryFeedKafkaConsumer.class);
  private KafkaConsumerConfigurationProperties config;
  private SimpleDateFormat dateFormat;
  private KafkaSender<K, V> dlqSender;
  private KafkaSender<K, V> sender;
  @Autowired
  private KafkaConsumerConfig kafkaConsumerConfig;
  @Autowired
  private KafkaProducerConfig kafkaProducerConfig;
  @Autowired
  private InventoryFeedKafkaProducer inventoryFeedKafkaProducer;

  @Value("${kafka.consumer.commit.interval:5s}")
  private Duration commitInterval;
  @Autowired
  protected Environment env;


  @EventListener(ApplicationReadyEvent.class)
  protected void consume() {

    SenderOptions<K, V> senderOptions = SenderOptions.create(kafkaProducerConfig.getProducer());
    dlqSender = KafkaSender.create(senderOptions);
    sender = KafkaSender.create(senderOptions);
    dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");
    Map<String, Object> consumerProps = new HashMap<>();
    final  String topicName =  "Customer1-custom-inv-integration-topic-US";
    config = generateKafkaConsumerConfigurationProperties(topicName);
    consumerProps.putAll(kafkaConsumerConfig.getConsumer());
    configureConsumerProperties(consumerProps, config.getGroupId());
    ReceiverOptions<K,V> receiverOptions =
        ReceiverOptions.<K, V>create(consumerProps)
            .subscription(Collections.singleton(topicName))
            .commitInterval(commitInterval)
            .addAssignListener(p -> LOG.info("Group partitions assigned: {}", p))
            .addRevokeListener(p -> LOG.info("Group partitions revoked: {}", p));
    receiveKafkaReceiver(receiverOptions,null);
  }

  private void receiveKafkaReceiver(ReceiverOptions<K,V> receiverOptions, Object o) {
    Flux<ReceiverRecord<K, V>> kafkaFlux = KafkaReceiver.create(receiverOptions).receive();
    kafkaFlux.subscribe(record->{
      ReceiverOffset offset =  record.receiverOffset();
      LOG.info("Received record: {}", record);
      InventoryKafkaPayload inventoryKafkaPayload = transformSupplyFeed(record);
      inventoryFeedKafkaProducer.postPayloadToTopic(inventoryKafkaPayload);
      });
  }

  private InventoryKafkaPayload transformSupplyFeed(ReceiverRecord<K,V> record) {
    InventoryKafkaPayload inventoryKafkaPayload = new InventoryKafkaPayload();
    InventoryUpdateKey key = record.key()==null?null:(InventoryUpdateKey) record.key();
    InventoryUpdateValue value = record.value()==null?null:(InventoryUpdateValue) record.value();
    if(key!=null||value!=null){
      inventoryKafkaPayload.setIsFullyQualifiedTopicName(true);
      inventoryKafkaPayload.setKey(key);
      inventoryKafkaPayload.setOperation("CREATE");
      inventoryKafkaPayload.setTopic("inventory-update-feed");
      InventoryUpdatePayload inventoryUpdatePayloadValue = transformUpdatePayload(value);
      inventoryKafkaPayload.setValue(inventoryUpdatePayloadValue);
    }
  return inventoryKafkaPayload;
  }

  private InventoryUpdatePayload transformUpdatePayload(InventoryUpdateValue value) {
    InventoryUpdatePayload updatedValue = new InventoryUpdatePayload();
    updatedValue.setOrgId(value.getOrgId());
    updatedValue.setLocationId(value.getLocationId());
    updatedValue.setLocationType(value.getLocationType());
    updatedValue.setEventType(value.getEventType());
    updatedValue.setFeedType(value.getFeedType());
    updatedValue.setProductId(value.getProductId());
    updatedValue.setUom(value.getUom());
    Double quantity = calculateQuantity(value.getTo());
    updatedValue.setQuantity(quantity);
    SupplyToPayload supplyToPayload = SupplyToPayload.builder().build();
    supplyToPayload.setSupplyType("ONHAND");
    updatedValue.setTo(supplyToPayload);
    updatedValue.setAudit(value.getAudit());
    updatedValue.setUpdateTimeStamp(value.getUpdateTimeStamp());
    updatedValue.setOverrideZoneTransitionRule(value.getOverrideZoneTransitionRule());
    return updatedValue;
  }

  private Double calculateQuantity(SupplyTo to) {
    Double onhandQuantity=0.0;
    for(SupplyType supply : to.getSupplyTypes()) {
      if(supply.getSupplyType().equalsIgnoreCase("SOH"))
      onhandQuantity+=supply.getQuantity();
      else
        onhandQuantity-=supply.getQuantity();
    }
    return onhandQuantity;
  }

  private KafkaConsumerConfigurationProperties generateKafkaConsumerConfigurationProperties(String topicName) {
    return KafkaConsumerConfigurationProperties.builder()
        .consumerEnabled(env.getProperty(String.format("kafka.consumer.override.%s.enabled", topicName), Boolean.class, true))
        .groupId(env.getProperty("override.group-id." + topicName))
        .build();
  }


  protected void configureConsumerProperties(Map<String, Object> consumerProps, String groupId) {
    consumerProps.put(JsonDeserializer.KEY_DEFAULT_TYPE, InventoryUpdateKey.class);
    consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, InventoryUpdateValue.class);
    if (groupId != null) {
      consumerProps.put(GROUP_ID_CONFIG, groupId);
    }
  }
}
