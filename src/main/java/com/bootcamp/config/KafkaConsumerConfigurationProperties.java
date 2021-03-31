package com.bootcamp.config;

import java.time.Duration;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KafkaConsumerConfigurationProperties {
  private boolean consumerEnabled;
  private Duration commitInterval;
  private int retryLimit;
  private int schedulerTimeToLive;
  private Duration initialBackOff;
  private Duration maxBackOff;
  private boolean dlqPublishEnabled;
  private String groupId;
}
