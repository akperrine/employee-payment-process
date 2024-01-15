package com.skllstorm.perrine.profits.launcher;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ProfitJobLauncher implements CommandLineRunner {
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private Job job;

    @Override
    public void run(String... args) throws Exception {
        JobParameters jobParams = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        log.info("RUNNING JOB WITH INCREMENT LEVEL {}", jobParams.getLong("time"));

        jobLauncher.run(job, jobParams);

    }
}