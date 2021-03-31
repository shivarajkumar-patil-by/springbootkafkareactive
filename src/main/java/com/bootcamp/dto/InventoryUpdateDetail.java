package com.bootcamp.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.time.LocalDate;
import javax.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
@Builder(toBuilder = true)
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class InventoryUpdateDetail {

  private String segment;

  private String zoneId;

  private String subZoneId;

  @NotBlank
  private String supplyType;

  private LocalDate supplyDate;


}

