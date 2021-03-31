package com.bootcamp.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class InventoryUpdateKey {
  private String orgId;
  private String productId;
  private String locationType;
  private String locationId;
  private String uom;
}
