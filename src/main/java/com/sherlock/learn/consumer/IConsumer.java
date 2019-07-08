package com.sherlock.learn.consumer;

import com.sherlock.learn.producerconsumer.message.Message;

public interface IConsumer {
	
	void consume(Message message);

}
