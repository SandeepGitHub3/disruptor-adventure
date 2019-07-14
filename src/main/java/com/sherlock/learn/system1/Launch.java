package com.sherlock.learn.system1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sherlock.learn.consumer.Dispatcher;
import com.sherlock.learn.producerconsumer.message.Message;
import com.sherlock.learn.producerconsumer.message.MessageType;
import com.sherlock.learn.producerconsumer.producer.Producer;

public class Launch {
	private static final Logger LOG = LogManager.getLogger(Launch.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		/*Using Blocking Queue since it is specifically design for Producer consumer pattern*/
		BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>(50000);

		Thread producerThreadA = new Thread(new Producer(MessageType.A, messageQueue, 1000), "ProducerA");
		Thread producerThreadB = new Thread(new Producer(MessageType.B, messageQueue, 1000), "ProducerB");
		Thread producerThreadC = new Thread(new Producer(MessageType.C, messageQueue, 1000), "ProducerC");
		Thread producerThreadD = new Thread(new Producer(MessageType.D, messageQueue, 1000), "ProducerD");
		Dispatcher dispatcher = new Dispatcher(messageQueue);
		Thread dispatcherThread = new Thread(dispatcher, "Dispatcher");
		producerThreadA.start();
		producerThreadB.start();
		producerThreadC.start();
		producerThreadD.start();
		
		producerThreadA.join();
		producerThreadB.join();
		producerThreadC.join();
		producerThreadD.join();
		
		dispatcherThread.start();

		// Let the simulation run for, 50 seconds
        Thread.sleep(50 * 1000);
		LOG.info("Result Set:{}",dispatcher.getResult());
		
		 // End of simulation - shut down gracefully
		System.exit(0);
	}
}
