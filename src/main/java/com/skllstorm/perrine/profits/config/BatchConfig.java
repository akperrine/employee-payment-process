package com.skllstorm.perrine.profits.config;

import com.skllstorm.perrine.profits.listener.EmployeeToDbSkipListener;
import com.skllstorm.perrine.profits.model.Employee;
import com.skllstorm.perrine.profits.model.EmployeePayment;
import com.skllstorm.perrine.profits.process.EmployeeProcessor;
import com.skllstorm.perrine.profits.process.PaymentProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.MongoPagingItemReader;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.data.builder.MongoPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@EnableBatchProcessing
@Slf4j
@Configuration
public class BatchConfig {
    @Autowired
    private MongoTemplate template;


    @Bean
    public ItemReader<Employee> readFromCsv() {
        log.info("READING EMPLOYEES");
        String[] properties = new String[]{"X","employeeId", "firstName", "lastName", "jobTitle", "withheld401"};
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(properties);

        BeanWrapperFieldSetMapper<Employee> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setStrict(false);
        fieldSetMapper.setTargetType(Employee.class);

        DefaultLineMapper<Employee> lineMapper = new DefaultLineMapper<>();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(lineTokenizer);

        return new FlatFileItemReaderBuilder<Employee>()
                .name("readFromCsv")
                .resource(new ClassPathResource("mock_data.csv"))
                .lineMapper(lineMapper)
                .linesToSkip(1)
                .build();
    }

    @Bean
    public ItemProcessor<Employee, Employee> employeeProcessor(){
        log.info("PROCESSING EMPLOYEES");
        return new EmployeeProcessor();
    }

    @Bean
    public MongoItemWriter<Employee> writeEmployeeToMongo() {
        log.info("Writing EMPLOYEES");
        return new MongoItemWriterBuilder<Employee>()
                .template(template)
                .collection("employees")
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager
                      ) {
        log.info("STARTING JOB");
        return new StepBuilder("step1", jobRepository)
                .<Employee, Employee>chunk(10, transactionManager)
                .reader(readFromCsv())
                .processor(employeeProcessor())
                .writer(writeEmployeeToMongo())
                .faultTolerant()
                .skipLimit(10)
                .skip(FlatFileParseException.class)
                .listener(new EmployeeToDbSkipListener("fails.psv"))
                .build();
    }

    @Bean
    public MongoPagingItemReader<Employee> readFromMongo() {
        log.info("READING STEP 2");

        Query query = new Query();
        query.addCriteria(Criteria.where("lastName").regex("^[A-Pa-p][a-zA-Z]*$"));

        Map<String, Sort.Direction> sortMap = new HashMap<>();
        sortMap.put("lastName", Sort.Direction.ASC);

        return new MongoPagingItemReaderBuilder<Employee>()
                .name("readFromMongo")
                .template(template)
                .collection("employees")
                .query(query)
//                .jsonQuery("{}")
                .targetType(Employee.class)
                .sorts(sortMap)
                .build();
    }

    @Bean
    public ItemProcessor<Employee, EmployeePayment> paymentProcessor(){
        log.info("PROCESSING PAYMENTS");
        return new PaymentProcessor();
    }

    @Bean
    public FlatFileItemWriter<EmployeePayment> writeToCsv() {
        log.info("Writing STEP 2");

        return new FlatFileItemWriterBuilder<EmployeePayment>()
                .name("writeToCsv")
                .resource(new FileSystemResource("payments.csv"))
                .delimited()
                .names("employeeId", "firstName", "lastName", "totalPayment", "afterWithholding", "amountWithheld")
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager
    ) {
        log.info("STARTING STEP 2");
        return new StepBuilder("step2", jobRepository)
                .<Employee, EmployeePayment>chunk(10, transactionManager)
                .reader(readFromMongo())
                .processor(paymentProcessor())
                .writer(writeToCsv())
                .faultTolerant()
                .skipLimit(10)
                .skip(FlatFileParseException.class)
                .listener(new EmployeeToDbSkipListener("failed2.psv"))
                .build();
    }

    @Bean
    public Job paymentJob(JobRepository jobRepository, Step step1, Step step2) {
        return new JobBuilder("paymentJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .next(step2)
                .build();
    }

}
