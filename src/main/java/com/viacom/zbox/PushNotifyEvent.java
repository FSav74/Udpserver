/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

import java.net.InetAddress;
import java.sql.Timestamp;

/**
 *
 * @author Luca
 */
public class PushNotifyEvent {

    public int IDZBPushNotify;
    public int IDBlackBox;
    public Timestamp LastPushSent;
    public String SerialN;
    public InetAddress IP;
    public int Port = 0;
    public Timestamp LastContact;
    public zbox ZB;
}
