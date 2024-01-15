package com.skllstorm.perrine.profits.config;

import com.skllstorm.perrine.profits.model.Employee;
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
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionManager;

import java.util.stream.IntStream;

@EnableBatchProcessing
@Configuration
public class BatchConfig {
    @Autowired
    private MongoTemplate template;

    @Bean
    public ItemReader<Employee> readFromCsv() {
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
    public MongoItemWriter<Employee> writeEmployeeToMongo() {
        return new MongoItemWriterBuilder<Employee>()
                .template(template)
                .collection("employees")
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1", jobRepository)
                .<Employee, Employee>chunk(10, transactionManager)
                .reader(readFromCsv())
                .writer(writeEmployeeToMongo())
                .build();
    }

    @Bean
    public Job paymentJob(JobRepository jobRepository, Step step1) {
        return new JobBuilder("paymentJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .build();
    }

//
//    //1. Item Reader from CSV file
//    @Bean
//    public ItemReader<Product> reader(){
//        FlatFileItemReader<Product> reader=new FlatFileItemReader<Product>();
//        //loading file
//        reader.setResource(new ClassPathResource("myprods.csv"));
//
//        reader.setLineMapper(new DefaultLineMapper<>() {{
//            setLineTokenizer(new DelimitedLineTokenizer() {{
//                setNames("prodId","prodName","prodCost");
//            }});
//            setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
//                setTargetType(Product.class);
//            }});
//        }});
//
//
//        return reader;
//    }
//
//    //2. Item Processor
//    @Bean
//    public ItemProcessor<Product, Product> processor(){
//        return new ProductProcessor();
//    }
//
//
//    //#. Item Writer
//    @Bean
//    public ItemWriter<Product> writer(){
//        MongoItemWriter<Product> writer=new MongoItemWriter<>();
//        writer.setTemplate(template);
//        writer.setCollection("products");
//        return writer;
//    }
//
//    @Bean
//    public Step stepA(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
//        return new StepBuilder("stepA", jobRepository)
//                .<Product,Product>chunk(3, transactionManager)
//                .reader(reader())
//                .processor(processor())
//                .writer(writer()).build();
//    }
//
//    @Bean
//    public Job jobA(JobRepository jobRepository, Step stepA) {
//        return new JobBuilder("jobA", jobRepository)
//                .incrementer(new RunIdIncrementer())
//                .start(stepA)
//                .build();
//    }
}
