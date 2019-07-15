package com.sherlock.learn.producerconsumer.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.sherlock.learn.producerconsumer.message.Message;
import com.sherlock.learn.producerconsumer.message.MessageType;
import com.sherlock.learn.producerconsumer.util.MessagSystemConstants;

public class Dispatcher implements Runnable {
	private static final Logger LOG = LogManager.getLogger(Dispatcher.class);
	private List<Message> result = new CopyOnWriteArrayList<>();
	Consumer consumer1 = new Consumer("1");
	Consumer consumer2 = new Consumer("2");
	private Map<MessageType, Consumer> consumerMap = ImmutableMap.<MessageType, Consumer>builder()
			.put(MessageType.A, consumer1).put(MessageType.B, consumer1).put(MessageType.C, consumer2)
			.put(MessageType.D, consumer2).build();

	public String getResult() {
		return result.stream().map(Message::toString).collect(Collectors.joining());
	}

	protected BlockingQueue<Message> queue;

	public Dispatcher(BlockingQueue<Message> theQueue) {
		this.queue = theQueue;
	}

	@Override
	public void run() {
		StopWatch watch = new StopWatch();
		while (true) {
			try {
				Message message = queue.take();
				if (!consumerMap.containsKey(message.getType())) {
					LOG.error("Drop Message:{}.No Consumer Found", message);
				} else {
					/*
					 * COnsumer will collect upto 500 messages in a list. When this limit is reached
					 * it will simply print the results and empty the result for further processing
					 */
					if (result.size() == MessagSystemConstants.MAX_RESULT_SIZE) {
						/*
						 * calculate time taken to process 500 messages and reset timer for next batch
						 */
						watch.stop();
						LOG.info("Result:{}", result);
						LOG.info("TimeTaken:{}", watch.getTime());
						Thread.sleep(2 * 1000);
						result.clear();
						watch.reset();
					}
					/* Start Stopwatch when collecting first message */
					if (CollectionUtils.isEmpty(result)) {
						watch.start();
					}
					consumerMap.get(message.getType()).consume(message);
					result.add(message);
				}

			} catch (InterruptedException ex) {
				LOG.info("CONSUMER INTERRUPTED");
			}
		}
	}
}
