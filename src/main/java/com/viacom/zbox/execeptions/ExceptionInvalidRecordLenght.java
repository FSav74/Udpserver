/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox.execeptions;

/**
 *
 * @author Luca
 */
public class ExceptionInvalidRecordLenght extends Exception {

    /**
     * Creates a new instance of
     * <code>ExceptionInvalidRecordLenght</code> without detail message.
     */
    public ExceptionInvalidRecordLenght() {
    }

    /**
     * Constructs an instance of
     * <code>ExceptionInvalidRecordLenght</code> with the specified detail
     * message.
     *
     * @param msg the detail message.
     */
    public ExceptionInvalidRecordLenght(String msg) {
        super(msg);
    }
}
