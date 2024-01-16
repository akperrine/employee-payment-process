package com.skllstorm.perrine.profits.process;

import com.skllstorm.perrine.profits.model.Employee;
import org.springframework.batch.item.ItemProcessor;

public class EmployeeProcessor implements ItemProcessor<Employee, Employee> {
    @Override
    public Employee process(Employee item) throws Exception {
        if (item.getJobTitle().equals("contractors") || item.getJobTitle().equals("chairman")) {
            return null;
        } else {
            return item;
        }
    }
}
