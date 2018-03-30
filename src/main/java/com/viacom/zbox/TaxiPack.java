/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import org.bouncycastle.util.encoders.Hex;

/**
 *
 * @author Luca
 */
public class TaxiPack {

    static byte ID_PK_OK = 0x02;
    static byte ID_PK_NOK = 0x05;
    static byte ID_PK_Localization = 0x51;
    static byte ID_PK_Event = 0x52;
    static byte ID_PK_SendCommand = 0x63;
    static byte ID_PK_ReceiveReplay = 0x64;

    public byte[] SendLocalization(byte[] SNZB, byte StateZB, Timestamp TL, double Lat, double Long) {
        byte[] Payload = new byte[16];
        System.out.println("SNZB: A1B2C3D4" + " StateZB: " + StateZB + " date: " + TL.toGMTString() + " Lat: " + Lat + " Long: " + Long);

        int i = 0;
        Payload[i] = StateZB;
        i++;
        i += 3;
        byte[] DateTime = GenerateDateTime(TL);
        java.lang.System.arraycopy(DateTime, 0, Payload, i, 4);
        i += 4;
        byte[] CoordLat = GenerateCoord(Lat);
        java.lang.System.arraycopy(CoordLat, 0, Payload, i, 4);
        i += 4;
        byte[] CoordLong = GenerateCoord(Long);
        java.lang.System.arraycopy(CoordLong, 0, Payload, i, 4);
        i += 4;

        return SendPacket(SNZB, ID_PK_Localization, (byte) 0, Payload);

    }

    public byte[] SendEvent(byte[] SNZB, int IDEvent, byte Type, Timestamp TL, double Lat, double Long, byte[] Extra) {
        System.out.println("SNZB: " + (new String(Hex.encode(SNZB))).toUpperCase());
        System.out.println("IDEvent: " + IDEvent);
        System.out.println("Type: " + Type);
        System.out.println("date: " + TL.toGMTString());
        System.out.println("Lat: " + Lat);
        System.out.println("Long: " + Long);

        byte[] Payload = new byte[32];
        int i = 0;

        Payload[i] = (byte) (IDEvent >> 24);
        i++;
        Payload[i] = (byte) (IDEvent >> 16);
        i++;
        Payload[i] = (byte) (IDEvent >> 8);
        i++;
        Payload[i] = (byte) (IDEvent);
        i++;

        byte[] DateTime = GenerateDateTime(TL);
        java.lang.System.arraycopy(DateTime, 0, Payload, i, 4);
        i += 4;
        byte[] CoordLat = GenerateCoord(Lat);
        java.lang.System.arraycopy(CoordLat, 0, Payload, i, 4);
        i += 4;
        byte[] CoordLong = GenerateCoord(Long);
        java.lang.System.arraycopy(CoordLong, 0, Payload, i, 4);
        i += 4;

        Payload[i] = (byte) (Type);
        i++;
        java.lang.System.arraycopy(Extra, 0, Payload, i, 10);
        i += 10;


        return SendPacket(SNZB, ID_PK_Event, (byte) 0, Payload);
    }

    public static String toHexString(byte[] array) {
        return DatatypeConverter.printHexBinary(array);
    }

    public static byte[] toByteArray(String s) {
        return DatatypeConverter.parseHexBinary(s);
    }

    public byte[] SendReceiveReply(byte[] SNZB, byte[] data) {
        System.out.println("SNZB: " + toHexString(SNZB));

        byte Size = (byte) ((3 + data.length + 1) >> 4);
        byte Fill;
        byte Resto = (byte) ((3 + data.length + 1) & 0xF);
        if (Resto != 0) {
            Size++;
        }
        Fill = (byte) ((Size << 4) - (3 + data.length + 1));


        byte[] Payload = new byte[3 + data.length + 1 + Fill];
        Payload[0] = 0;
        Payload[1] = (byte) data.length;
        Payload[2] = 0;

        java.lang.System.arraycopy(data, 0, Payload, 3, data.length);

        MessageDigest cript;

        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(Payload, 0, 3 + data.length + Fill);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(TaxiPack.class.getName()).log(Level.SEVERE, null, ex);
            return Payload;
        }

        byte[] CRC = cript.digest();

        Payload[3 + data.length + Fill] = CRC[0];

        return SendPacket(SNZB, ID_PK_ReceiveReplay, (byte) 0, Payload);
    }

    public byte[] SendPacket(byte[] SessionID, byte Type, byte Crypt, byte[] Payload) {
        byte Buffer[] = new byte[1024];
        int i = 0;

        byte Size = (byte) (Payload.length >> 4);
        byte Fill;
        byte Resto = (byte) (Payload.length & 0xF);
        if (Resto != 0) {
            Size++;
        }
        Fill = (byte) ((Size << 4) - Payload.length);

        Buffer[i] = (byte) 0xA5;
        i++;
        Buffer[i] = (byte) 0x5A;
        i++;
        java.lang.System.arraycopy(SessionID, 0, Buffer, 2, 4);
        i += 4;
        Buffer[i] = 0;
        i++;                                                                    // PackN
        Buffer[i] = Type;
        i++;                                                               //Type
        Buffer[i] = Size;
        i++;								// size
        Buffer[i] = (byte) ((Crypt << 7) + Fill);
        i++;            				// Crypt/Fill

        System.arraycopy(Payload, 0, Buffer, i, Payload.length);        			// Payload

        i += Payload.length;
        i += Fill;


        int nByte = i;
        System.out.println("ID= " + SessionID + " SendGPRSData nByte=" + nByte + " i=" + i);

        MessageDigest cript;

        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(Buffer, 0, nByte);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(TaxiPack.class.getName()).log(Level.SEVERE, null, ex);
            return Buffer;
        }

        byte[] CRC = cript.digest();

        Buffer[i] = CRC[0];
        i++;
        Buffer[i] = CRC[1];
        i++;
        Buffer[i] = CRC[2];
        i++;

//        byte[] HexStr=new byte[13+Payload.length];
//        java.lang.System.arraycopy(Buffer, 0, HexStr, 0, 13+Payload.length );
//        byte OutBuffer[]= new byte [13+Payload.length];
//        java.lang.System.arraycopy(Buffer, 0, OutBuffer, 0, 13+Payload.length );
        byte[] HexStr = new byte[i];
        java.lang.System.arraycopy(Buffer, 0, HexStr, 0, i);
        byte OutBuffer[] = new byte[i];
        java.lang.System.arraycopy(Buffer, 0, OutBuffer, 0, i);

        System.out.println(Utils.toHexString(HexStr));

        return OutBuffer;
    }

    byte[] GenerateDateTime(Timestamp TS) {
        byte[] DateTime = new byte[4];
        DateTime[0] = (byte) (((TS.getYear() - 2000) << 1) + ((TS.getMonth() + 1) >> 3));
        DateTime[1] = (byte) (((TS.getMonth() + 1) << 5) + ((TS.getDay() & 0x1F)));
        DateTime[2] = (byte) ((TS.getHours() << 3) + ((TS.getMinutes()) >> 3));
        DateTime[3] = (byte) (((TS.getMinutes()) << 5) + (((TS.getSeconds() >> 1) & 0x1F)));

        return DateTime;
    }

    byte[] GenerateCoord(double Coord_Double) {
        int Gradi = (int) Math.abs(Coord_Double);
        int Decml = (int) ((Math.abs(Coord_Double) - Gradi) * 10000);

        byte[] Coord = new byte[4];
        Coord[0] = (byte) (Gradi);
        if (Coord_Double > 0) {
            Coord[1] = (byte) ((1 << 7) + (Decml >> 16));
        } else {
            Coord[1] = (byte) ((0 << 7) + (Decml >> 16));
        }
        Coord[2] = (byte) ((Decml >> 8) & 0xFF);
        Coord[3] = (byte) ((Decml) & 0xFF);
        return Coord;
    }
}
