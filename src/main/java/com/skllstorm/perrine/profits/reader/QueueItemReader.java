package com.skllstorm.perrine.profits.reader;

import com.skllstorm.perrine.profits.service.ItemQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Iterator;

@Slf4j
public class QueueItemReader<T> implements ItemReader<T> {

    @Autowired
    ItemQueue<T> itemQueue;

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        T item = itemQueue.poll();
        log.info("READING queue of size: {}, item: {}", itemQueue.size(), item);
        return item;
    }

}
