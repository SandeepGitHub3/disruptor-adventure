package com.sherlock.learn.producerconsumer.javaqueues;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sherlock.learn.producerconsumer.consumer.Dispatcher;
import com.sherlock.learn.producerconsumer.message.Message;
import com.sherlock.learn.producerconsumer.message.MessageType;

public class JavaQueuesSystemLauncher {
	private static final Logger LOG = LogManager.getLogger(JavaQueuesSystemLauncher.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		/*
		 * Using Blocking Queue since it is specifically design for Producer consumer
		 * pattern
		 */
		BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>(500);

		/* Start Consumer Thread */
		Dispatcher dispatcher = new Dispatcher(messageQueue);
		Thread dispatcherThread = new Thread(dispatcher, "Dispatcher");
		dispatcherThread.start();

		/* Start producer Threads */
		produceMessages(messageQueue, MessageType.A);
		produceMessages(messageQueue, MessageType.B);
		produceMessages(messageQueue, MessageType.C);
		produceMessages(messageQueue, MessageType.D);

		/* Let the simulation run for 20 secs */
		Thread.sleep(20 * 1000);

		/* End of simulation - shut down gracefully */
		System.exit(0);
	}

	static void produceMessages(BlockingQueue<Message> messageQueue, MessageType messageType) {
		/* Message Producer threads */
		CompletableFuture.runAsync(() -> {
			for (int l = 0; true; l++) {
				// bb.putInt(0, l);
				Message m = new Message(messageType, l);
				try {
					// LOG.info("Put Message:{}",m);
					messageQueue.put(m);
				} catch (InterruptedException e) {
					LOG.info("Producer INTERRUPTED");
				}
			}
		});
	}
}
