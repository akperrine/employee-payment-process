package com.skllstorm.perrine.profits.writer;

import com.skllstorm.perrine.profits.model.EmployeePayment;
import com.skllstorm.perrine.profits.service.ItemQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class QueueItemWriter<T> implements ItemWriter<T> {

    @Autowired
    private ItemQueue<T> itemQueue;


    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
            log.info("{}", chunk.toString());
        for (T item : chunk) {
            log.info("Item being written to QUEUE: {}", item.toString());
            itemQueue.add(item);
            log.info("Item logged successfully {}", item.toString());
        }
    }
}
