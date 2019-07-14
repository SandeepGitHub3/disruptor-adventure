package com.sherlock.learn.system1;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.sherlock.learn.consumer.Consumer;
import com.sherlock.learn.producerconsumer.message.Message;
import com.sherlock.learn.producerconsumer.message.MessageType;

public class DisruptorSystemLauncher {
	private static final Logger LOG = LogManager.getLogger(DisruptorSystemLauncher.class);
	static Consumer consumer1 = new Consumer("1");
	static Consumer consumer2 = new Consumer("2");
	private static Map<MessageType, Consumer> consumerMap = ImmutableMap.<MessageType, Consumer>builder()
			.put(MessageType.A, consumer1).put(MessageType.B, consumer1).put(MessageType.C, consumer2)
			.put(MessageType.D, consumer2).build();

	public static void main(String[] args) throws InterruptedException, TimeoutException {
		List<Message> result = new CopyOnWriteArrayList<>();
		Disruptor<Message> disruptor = new Disruptor<>(Message::new, 1024, DaemonThreadFactory.INSTANCE);
		
		StopWatch watch = new StopWatch();
		disruptor.handleEventsWith((message, sequence, endOfBatch) -> {
			if (!consumerMap.containsKey(message.getType())) {
				LOG.error("Drop Message:{}.No Consumer Found", message);
			} else {
				if (result.size() == 500) {
					watch.stop();
					LOG.info("Result:{}", result);
					LOG.info("TimeTaken:{}", watch.getTime());
					Thread.sleep(5*1000);
					result.clear();
					watch.reset();
					watch.start();
				}
				LOG.info("Process Message: " + message);
				consumerMap.get(message.getType()).consume(message);
				result.add(message);
			}
		});
		
		disruptor.start();

		ByteBuffer bb = ByteBuffer.allocate(8);
		produceMessages(disruptor, bb, MessageType.A);
		produceMessages(disruptor, bb, MessageType.B);
		produceMessages(disruptor, bb, MessageType.C);
		produceMessages(disruptor, bb, MessageType.D);
		watch.start();
		
		Thread.sleep(100*1000);
	}

	static void produceMessages(Disruptor<Message> disruptor, ByteBuffer bb, MessageType messageType) {
		CompletableFuture.runAsync(() -> {
			for (int l = 0; true; l++) {
				bb.putInt(0, l);
				disruptor.getRingBuffer().publishEvent((message, sequence, buffer) -> {
					message.setType(messageType);
					message.setMessageNumber(buffer.getInt(0));
				}, bb);
			}
		});
	}
}
