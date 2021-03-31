package com.bootcamp.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Setter
@Getter
public class SupplyToPayload {
  String supplyType;
}
