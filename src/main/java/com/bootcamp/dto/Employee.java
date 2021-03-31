package com.bootcamp.dto;


import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Employee {

  @NotNull(message = "employeeId cannot be null")
  @Pattern(regexp = "[0-9]+", message = "employeeId is invalid")
  private String employeeId;
  @NotNull(message = "employeeName cannot be null")
  @NotEmpty(message = "employeeName cannot be empty")
  private String employeeName;
  @NotNull(message = "address cannot be null")
  private String address;
  @NotNull(message = "organization cannot be null")
  private String organization;
  @NotNull(message = "role cannot be null")
  private String role;

}
