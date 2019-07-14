package com.sherlock.learn.producerconsumer.message;

public class Message {
	private MessageType type;
	private int messageNumber;
	private boolean isProcessed;
	
	public Message() {
	}

	public Message(MessageType type,int messageNumber) {
		super();
		this.type = type;
		this.messageNumber=messageNumber;
	}

	@Override
	public String toString() {
		String messageDescription=type.name()+messageNumber;
		if(isProcessed)
			messageDescription=messageDescription.concat("P");
		return messageDescription;
	}

	public MessageType getType() {
		return type;
	}

	public void setProcessed(boolean isProcessed) {
		this.isProcessed = isProcessed;
	}

	public void setType(MessageType type) {
		this.type = type;
	}

	public void setMessageNumber(int messageNumber) {
		this.messageNumber = messageNumber;
	}

}
