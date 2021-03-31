package com.bootcamp.config;

import com.bootcamp.serialization.JacksonSerializer;
import javax.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.support.serializer.JsonSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
@Configuration
@ConfigurationProperties(prefix = "kafka")
@Getter
@Setter
@RequiredArgsConstructor
public class KafkaProducerConfig {
  private final String keySerializerClassName;
  private final String valueSerializerClassName;
  private final String partitionerClassName;

  protected Map<String, Object> producer;

  public KafkaProducerConfig() {
    keySerializerClassName = JacksonSerializer.class.getName();
    valueSerializerClassName = JacksonSerializer.class.getName();
    partitionerClassName = null;
  }

  @PostConstruct
  private void init() {
    if (producer == null) {
      producer = new HashMap<>();
    } else {
      producer = flatten(producer);
      setKeySerializer();
      setValueSerializer();
      setPartitioner();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> flatten(Map<String, Object> map) {
    Map<String, Object> result = new HashMap<>();
    map.forEach((key, value) -> {
      if (value instanceof Map) {
        Map<String, Object> nestedMap = flatten((Map<String, Object>) value);
        nestedMap.forEach((nestedKey, nestedValue) ->
            result.put(key + "." + nestedKey, nestedValue));
      } else {
        result.put(key, value);
      }
    });
    return result;
  }


  private void setKeySerializer() {
    producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
  }

  private void setValueSerializer() {
    producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
  }

  private void setPartitioner() {
    if (partitionerClassName != null) {
      producer.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClassName);
    }
  }
  }