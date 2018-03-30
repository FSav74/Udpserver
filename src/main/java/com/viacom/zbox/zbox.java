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
public class zbox {

    public int IDBlackBox;
    public byte[] SerialN = new byte[4];
    public long SerialN_N;
    public String NumTel = "-----";
    public String Descr = "-----";
    public String Targa = "-----";
    public byte[][] AESRootKey = new byte[][]{
        {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, {(byte) 0x8B, (byte) 0x17, (byte) 0x61, (byte) 0x3F, (byte) 0x77, (byte) 0x35, (byte) 0x59, (byte) 0xCD,
            (byte) 0xEF, (byte) 0xA4, (byte) 0xDE, (byte) 0x0B, (byte) 0xD9, (byte) 0x4F, (byte) 0x38, (byte) 0xA6}
    };
    public byte[] AESKeyIn = new byte[16];
    public byte[] AESKeyOut = new byte[16];
    public InetAddress IP;
    public int Port = 0;
    public int ProtV = 0;
    public int IDZBox;
    public int PackNRef = 0;
    public int StatoConnessione = 0;
    public int IDAzienda = 0;
    public boolean SubNet[];
    public int CertidriveIDVeicolo;
    public int IDSWVersion;
    public TokenAuths TKAs_Acknoledge = null;
    public int AutoAccFileDownload=0;

    public zbox() {
        java.util.Arrays.fill(SerialN, (byte) 0);
//        java.util.Arrays.fill(AESRootKey,(byte)0);
        java.util.Arrays.fill(AESKeyIn, (byte) 0);
        java.util.Arrays.fill(AESKeyOut, (byte) 0);
    }

    public long GetZBCod() {
        long Cod = (Utils.uBToL(SerialN[0]) << 24)
                + (Utils.uBToL(SerialN[1]) << 16)
                + (Utils.uBToL(SerialN[2]) << 8)
                + (Utils.uBToL(SerialN[3]));
        return Cod;
    }
}
