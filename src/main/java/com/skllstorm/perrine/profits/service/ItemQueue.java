package com.skllstorm.perrine.profits.service;

import com.skllstorm.perrine.profits.model.EmployeePayment;
import org.springframework.stereotype.Service;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class ItemQueue<T> extends AbstractQueue<T>{
    private LinkedList<T> items;

    public ItemQueue() {
        this.items = new LinkedList<>();
    }

    @Override
    public Iterator iterator() {
        return items.iterator();
    }

    @Override
    public int size() {
        return items.size();
    }

    @Override
    public boolean offer(T item) {
        if (item == null) return false;
        items.add(item);
        return true;
    }


    @Override
    public T poll() {
       Iterator<T> iter = items.iterator();
       while (iter.hasNext()) {
           T t = iter.next();
           if (t != null) {
               iter.remove();
               return t;
             }
       }
       return null;
    }

    @Override
    public T peek() {
        return items.getFirst();
    }
}
