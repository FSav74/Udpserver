/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox.execeptions;

/**
 *
 * @author Luca
 */
public class ExceptionCRCError extends Exception {

    /**
     * Creates a new instance of
     * <code>ExceptionCRCError</code> without detail message.
     */
    public ExceptionCRCError() {
    }

    /**
     * Constructs an instance of
     * <code>ExceptionCRCError</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public ExceptionCRCError(String msg) {
        super(msg);
    }
}
