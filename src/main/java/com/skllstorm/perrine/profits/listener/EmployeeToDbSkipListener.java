package com.skllstorm.perrine.profits.listener;

import com.skllstorm.perrine.profits.model.Employee;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class EmployeeToDbSkipListener implements SkipListener<Employee, Employee> {

    Path skippedItemsFile;

    //Listener will write to file if comes across a FlatFileParseException
    public EmployeeToDbSkipListener(String skippedItemsFile) {
        this.skippedItemsFile = Paths.get(skippedItemsFile);
    }

    @Override
    public void onSkipInRead(Throwable throwable) {
        if (throwable instanceof FlatFileParseException exception) {
            String rawLine = exception.getInput();
            int lineNumber = exception.getLineNumber();
            String skippedLine = lineNumber + "|" + rawLine + System.lineSeparator();

            try{
                Files.writeString(this.skippedItemsFile, skippedLine, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            }
            catch (IOException e) {
                throw new RuntimeException("Unable to write file " + skippedLine);
            }
        }
    }
}
