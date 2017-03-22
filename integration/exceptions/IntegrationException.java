package com.cg.syscab.integration.exceptions;

public class IntegrationException extends Exception{
	private String messageKey;
	public IntegrationException(String messageKey){
		this.messageKey = messageKey;
	}
	public String getMessageKey() {
		return messageKey;
	}
	public void setMessageKey(String messageKey) {
		this.messageKey = messageKey;
	}
}
