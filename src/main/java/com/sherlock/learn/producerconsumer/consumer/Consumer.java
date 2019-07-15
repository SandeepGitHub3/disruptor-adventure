package com.sherlock.learn.producerconsumer.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sherlock.learn.producerconsumer.message.Message;

public class Consumer implements IConsumer {
	private static final Logger LOG = LogManager.getLogger(Consumer.class);
	private String consumerName;

	public Consumer(String consumerName) {
		super();
		this.consumerName = consumerName;
	}

	@Override
	public String toString() {
		return "Consumer [" + consumerName + "]";
	}

	@Override
	public void consume(Message message) {
		// LOG.info("Consumer:{} -Process-{} ",consumerName, message);
		message.setProcessed(true);
	}
}
