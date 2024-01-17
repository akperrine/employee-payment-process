package com.skllstorm.perrine.profits.process;

import com.skllstorm.perrine.profits.model.Employee;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class EmployeeProcessor implements ItemProcessor<Employee, Employee> {
    //Simple filter process
    @Override
    public Employee process(Employee item) throws Exception {
        if (item.getJobTitle().equals("contractors") || item.getJobTitle().equals("chairman")) {
        log.info("REMOVING CONTRACTORS AND CHAIRMEN {}: {}", item.getLastName(), item.getJobTitle());
            return null;
        } else {
            return item;
        }
    }
}
