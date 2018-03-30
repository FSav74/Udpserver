/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

/**
 *
 * @author Salvatore
 */
public class DownloadEnd {

    public long IdFile;
    public long Size;
    public byte ErrorCode;
    public byte[] Hash = new byte[20];
}
