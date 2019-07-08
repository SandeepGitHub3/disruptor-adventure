package com.sherlock.learn.producerconsumer.producer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sherlock.learn.producerconsumer.message.Message;
import com.sherlock.learn.producerconsumer.message.MessageType;

public class Producer implements Runnable{
	private static final Logger LOG = LogManager.getLogger(Producer.class);
	private MessageType type;
	private int maxMessages;
	private AtomicInteger counter= new AtomicInteger(0);
	
	protected BlockingQueue<Message> queue;
	 
    public Producer(MessageType type,BlockingQueue<Message> theQueue,int maxMessages) {
    	this.type= type;
        this.queue = theQueue;
        this.maxMessages= maxMessages;
    }
 
    public void run()
    {
        try
        {
            while (counter.get()<=maxMessages)
            {
            	Message justProduced = getResource();
                queue.put(justProduced);
                LOG.info("Producer:{} Message-->{} .Queue size now ={} ", this.type.name(),justProduced,queue.size());
            }
        }
        catch (InterruptedException ex)
        {
        	LOG.info("Producer INTERRUPTED");
        }
    }
 
    Message getResource()
    {
        return new Message(this.type,counter.getAndIncrement());
    }
    
    @Override
	public String toString() {
		return "Producer [type=" + type + "]";
	}
}
