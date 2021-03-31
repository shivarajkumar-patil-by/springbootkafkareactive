package com.bootcamp.validator;

import com.bootcamp.dto.Employee;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.springframework.stereotype.Component;

@Component
public abstract class DataValidator {

   public Validator configureValidator(){
     ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
     Validator validator = factory.getValidator();
     return validator;
   }

  public abstract void validate(Employee employee) throws ClassNotFoundException;
}
