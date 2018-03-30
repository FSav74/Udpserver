/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

/**
 *
 * @author Salvatore
 */
public class DownloadChunkData {

    public int ByteCount;
    public int ChunkNum;
    public long IDFile;
    public byte[] ChunkData = new byte[1000];
}
