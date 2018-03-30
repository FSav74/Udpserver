/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.gruppoac.udpserver.kafkaproducer.exception;

/**
 *
 * @author Admin
 */
public class ProducerException extends Exception {
    
    	public ProducerException() {
	
		super();
	}

	/**
	 * @param message
	 */
	public ProducerException(String message) {

		super(message);
	}

	/**
	 * @param cause
	 */
	public ProducerException(Throwable cause) {
	
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ProducerException(String message, Throwable cause) {
	
		super(message, cause);
	}
}
