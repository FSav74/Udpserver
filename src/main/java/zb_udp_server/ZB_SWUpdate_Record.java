/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import java.sql.Timestamp;

/**
 *
 * @author Luca
 */
public class ZB_SWUpdate_Record {

    public long IDSWUpProcess = -1;
    public long IDBlackBox = -1;
    public int Stato = -1;
    public Timestamp LastCheck;
    public long IDCommandDownload = -1;
    public long IDCommandSWUP = -1;
    public long IDZBFileDownload = -1;
    public long IDZBFileDownloadSession = -1;
    public Timestamp LastSentChunk_Date;
    public Timestamp LastCommandSent_Date;
    public String FileName;
    public long IDCommandPre;
    public String CommandPre;
    public long IDCommandPost;
    public String CommandPost;
}
