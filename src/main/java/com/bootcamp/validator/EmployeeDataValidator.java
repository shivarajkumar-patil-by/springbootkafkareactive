package com.bootcamp.validator;

import com.bootcamp.dto.Employee;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import org.springframework.stereotype.Component;

@Component
public class EmployeeDataValidator extends DataValidator{

  public void validate(Employee employee) {
    Set<ConstraintViolation<Employee>> violations = configureValidator().validate(employee);
    if(!violations.isEmpty())
      throw new ConstraintViolationException(violations);
  }
}
