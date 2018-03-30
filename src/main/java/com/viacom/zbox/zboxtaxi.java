/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

import java.net.InetAddress;

/**
 *
 * @author Luca
 */
public class zboxtaxi {

    public int IDZBox;
    public int IDBlackBox;
    public byte[] SerialN = new byte[4];
    public String NumTel = "-----";
    public String Descr = "-----";
    public String Targa = "-----";
    public InetAddress IP;
    public int Port = 0;
    public int ProtV = 0;
    public int PackNRef = 0;
    public int StatoConnessione = 0;
    public int IDAzienda = 0;
    public boolean SubNet[];
    public int CertidriveIDVeicolo;
    public int IDSWVersion;
    public java.sql.Timestamp LastContact;
    public long LastIDZBLocalizationSent;
    public long LastIDZBEventSent;
    public InetAddress ConnIPStart;
    public int ConnPortStart;

    public zboxtaxi() {
        java.util.Arrays.fill(SerialN, (byte) 0);
    }

    public long GetZBCod() {
        long Cod = (Utils.uBToL(SerialN[0]) << 24)
                + (Utils.uBToL(SerialN[1]) << 16)
                + (Utils.uBToL(SerialN[2]) << 8)
                + (Utils.uBToL(SerialN[3]));
        return Cod;
    }
}
