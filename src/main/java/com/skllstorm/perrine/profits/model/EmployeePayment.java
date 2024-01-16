package com.skllstorm.perrine.profits.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
@AllArgsConstructor
public class EmployeePayment {

    @Id
    private String employeeId;
    private String firstName;
    private String lastName;
    private double totalPayment;
    private double afterWithholding;
    private double amountWithheld;
}
