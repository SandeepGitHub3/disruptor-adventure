package com.sherlock.learn.producerconsumer.disruptorsystem;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.sherlock.learn.producerconsumer.consumer.Consumer;
import com.sherlock.learn.producerconsumer.message.Message;
import com.sherlock.learn.producerconsumer.message.MessageType;
import com.sherlock.learn.producerconsumer.util.MessagSystemConstants;

public class DisruptorSystemLauncher {
	private static final Logger LOG = LogManager.getLogger(DisruptorSystemLauncher.class);
	private static Consumer consumer1 = new Consumer("1");
	private static Consumer consumer2 = new Consumer("2");
	private static Map<MessageType, Consumer> consumerMap = ImmutableMap.<MessageType, Consumer>builder()
			.put(MessageType.A, consumer1).put(MessageType.B, consumer1).put(MessageType.C, consumer2)
			.put(MessageType.D, consumer2).build();

	public static void main(String[] args) throws InterruptedException, TimeoutException {
		List<Message> result = new CopyOnWriteArrayList<>();
		/* Initialize Disruptor with size of 1024 */
		Disruptor<Message> disruptor = new Disruptor<>(Message::new, 1024, DaemonThreadFactory.INSTANCE);

		StopWatch watch = new StopWatch();

		/* Define Consumer */
		disruptor.handleEventsWith((message, sequence, endOfBatch) -> {
			if (!consumerMap.containsKey(message.getType())) {
				LOG.error("Drop Message:{}.No Consumer Found", message);
			} else {
				/*
				 * Consumer will collect upto 500 messages in a list. When this limit is reached
				 * it will simply print the results and empty the result for further processing
				 */
				if (result.size() ==MessagSystemConstants.MAX_RESULT_SIZE) {
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
				/* Collect processed messages in a list */
				result.add(message);
			}
		});

		disruptor.start();

		ByteBuffer bb = ByteBuffer.allocate(8);
		produceMessages(disruptor, bb, MessageType.A);
		produceMessages(disruptor, bb, MessageType.B);
		produceMessages(disruptor, bb, MessageType.C);
		produceMessages(disruptor, bb, MessageType.D);

		/* Let the simulation run for 20 secs */
		Thread.sleep(20 * 1000);

		/* End of simulation - shut down gracefully */
		System.exit(0);
	}

	static void produceMessages(Disruptor<Message> disruptor, ByteBuffer bb, MessageType messageType) {
		/* Message Producer threads */
		CompletableFuture.runAsync(() -> {
			for (int l = 0; true; l++) {
				bb.putInt(0, l);
				disruptor.getRingBuffer().publishEvent((message, sequence, buffer) -> {
					message.setType(messageType);
					message.setMessageNumber(buffer.getInt(0));
					//LOG.info("Produce Message:{}",message);
				}, bb);
			}
		});
	}
}
