package com.bootcamp.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Audit {
  private String transactionId;
  private String transactionReason;
  private String transactionSystem;
  private String transactionType;
  private String transactionUser;
}

