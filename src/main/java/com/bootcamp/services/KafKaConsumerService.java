package com.bootcamp.services;

import com.bootcamp.dto.Employee;
import com.bootcamp.dto.InventoryKafkaPayload;
import com.bootcamp.dto.InventoryUpdateKey;
import com.bootcamp.dto.InventoryUpdatePayload;
import com.bootcamp.validator.EmployeeDataValidator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class KafKaConsumerService
{
 /* private final Logger LOG = LoggerFactory.getLogger(KafKaConsumerService.class);

  @Autowired
  KafKaProducerService kafKaProducerService;
  @Autowired
  DlqErrorRecordService dlqErrorRecordService;
  @Autowired
  EmployeeDataValidator employeeDataValidator;

  @KafkaListener(topics = "employee")
  public void consumeFromTopicEmployee(String employee) {
    LOG.info("Employee info received from topic(employee): " + employee);
    try {
      Employee empObject = new ObjectMapper().readValue(employee, Employee.class);
      employeeDataValidator.validate(empObject);
      kafKaProducerService.post(empObject, empObject.getEmployeeId(), "employeeTarget");
    } catch (ConstraintViolationException | JsonProcessingException e) {
      dlqErrorRecordService.handleValidationError(employee);
    }
  }
    @KafkaListener(topics = "custom-inventory-update-feed")
    public void consumeInventorySupplyFeed(InventoryUpdatePayload value) {
      LOG.info("Supply feed received from topic(custom-inventory-update-feed): {}", value);
      try {

//        kafKaProducerService.post(inventoryKafkaPayload, "1","inventory-update-feed");
      } catch (ConstraintViolationException e) {
        //
      }

  }

  @KafkaListener(topics = "custom-inv")
  public void consumeInventorySupplyFeedCustom(String value) {

    LOG.info("Supply feed received from topic(custom): {}", value);
    try {

//        kafKaProducerService.post(inventoryKafkaPayload, "1","inventory-update-feed");
    } catch (ConstraintViolationException e) {
      //
    }

  }

  @KafkaListener(topics = "Customer1-custom-inventory-update-feed-integration-topic-US")
  public void consumeInventorySupplyFeed1(InventoryKafkaPayload inventoryKafkaPayload) {
    LOG.info("Supply feed received from topic1(custom-inventory-update-feed): {}", inventoryKafkaPayload);
    try {
      kafKaProducerService.post(inventoryKafkaPayload, "1","inventory-update-feed");
    } catch (ConstraintViolationException e) {
      //
    }

  }

  @KafkaListener(topics = "employeeTarget")
  public void consumeFromTopicNextTaskTopic(Employee employee) {
    LOG.info("Employee info received from topic(employeeTarget): "+ employee);
  }

  @KafkaListener(topics = "employeeTarget")
  public void consumeFromTopicNextTaskTopicS(String employee) {
    LOG.info("Employee info received from topic(employeeTarget): "+ employee);
  }
*/}