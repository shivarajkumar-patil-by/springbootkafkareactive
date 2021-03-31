package com.bootcamp.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
@Builder(toBuilder = true)
@Setter
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class InventoryUpdateAudit {

  private String transactionType;

  private String transactionId;

  private String transactionReason;

  private String transactionUser;

  private String transactionSystem;
}

