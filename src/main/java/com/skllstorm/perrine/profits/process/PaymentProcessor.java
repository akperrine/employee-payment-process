package com.skllstorm.perrine.profits.process;

import com.skllstorm.perrine.profits.model.Employee;
import com.skllstorm.perrine.profits.model.EmployeePayment;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class PaymentProcessor implements ItemProcessor<Employee, EmployeePayment> {
    // calc process that creates an EmployeePayment class and calcs the income and witholdiing
    @Override
    public EmployeePayment process(Employee item) throws Exception {
        log.info("Logging item user {}", item.getLastName());
        double salary;
        switch (item.getJobTitle()) {
            case "business analyst":
                salary = 80_000;
                break;
            case "developer 1":
                salary = 90_000;
                break;
            case "developer 2":
                salary = 100_000;
                break;
            case "developer 3":
                salary = 120_000;
                break;
            case "technical writer":
                salary = 95_000;
            default:
                return null;
        }
        double payment = calcBiweeklyPayment(salary);
        double amountWithheld = payment * item.getWithheld401();
        double amountAfterWithholding = payment - amountWithheld;

        return new EmployeePayment(item.getEmployeeId(), item.getFirstName(), item.getLastName(),
                payment, amountAfterWithholding, amountWithheld);
    }

    private double calcBiweeklyPayment(double salary) {
        // divide by month and then by weeks per month and then make biweekly payment
        return salary / 12 / 4.2 * 2;
    }
}
