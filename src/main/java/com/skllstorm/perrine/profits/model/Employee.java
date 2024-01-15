package com.skllstorm.perrine.profits.model;

import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
public class Employee {
    @Id
    private String employeeId;
    private String firstName;
    private String lastName;
    private String jobTitle;
    private float withheld401;
}
