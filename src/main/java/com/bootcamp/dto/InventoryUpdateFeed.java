package com.bootcamp.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder(toBuilder = true)

public class InventoryUpdateFeed {

  @NotBlank
  private String orgId;

  @NotBlank
  private String productId;

  private String locationType;
  @NotBlank
  private String locationId;

  @NotNull
  private Double quantity;

  @NotBlank
  private String uom;

  @NotNull @Pattern(regexp = "ABSOLUTE|DELTA")
  private String feedType;

  @NotBlank
  private String eventType;

  private String passedEventType;

  private ZonedDateTime updateTimeStamp;

  private InventoryUpdateAudit audit;

  @JsonIgnore
  private LocalDateTime updateTime;
  @JsonIgnore
  private String updateUser;

  private InventoryUpdateDetail from;
  private InventoryUpdateDetail to;

}
