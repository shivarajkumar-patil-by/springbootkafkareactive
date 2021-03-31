package com.bootcamp.dto;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class InventoryKafkaPayload {
  private Boolean isFullyQualifiedTopicName;
  private InventoryUpdateKey key;
  private String operation;
  private String topic;
  private InventoryUpdatePayload value;
  //  private Audit audit;
}