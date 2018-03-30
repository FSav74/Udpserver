package com.viacom.zbox;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Luca
 */
public class Utils {

     public static String getStackTrace (Throwable t){
        StringWriter stringWriter = new StringWriter();
        PrintWriter  printWriter  = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        printWriter.close();    //surprise no IO exception here
        try {
            stringWriter.close();
        }
        catch (IOException e) {
        }
        return stringWriter.toString();
    }

    /**
     * Fast convert a byte array to a hex string with possible leading zero.
     *
     * @param b array of bytes to convert to string
     * @return hex representation, two chars per byte.
     */
    public static String toHexString(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (int i = 0; i < b.length; i++) {
            // look up high nibble char
            sb.append(hexChar[(b[i] & 0xf0) >>> 4]);

            // look up low nibble char
            sb.append(hexChar[b[i] & 0x0f]);
        }
        return sb.toString();
    }

    private static byte[] rotateLeft(byte[] in, int len, int step) {
        int numOfBytes = (len - 1) / 8 + 1;
        byte[] out = new byte[numOfBytes];
        for (int i = 0; i < len; i++) {
            int val = getBit(in, (i + step) % len);
            setBit(out, i, val);
        }
        return out;
    }

    public static int getBit(byte[] data, int pos) {
        int posByte = pos / 8;
        int posBit = pos % 8;
        byte valByte = data[posByte];
        int valInt = valByte >> (8 - (posBit + 1)) & 0x0001;
        return valInt;
    }

    private static void setBit(byte[] data, int pos, int val) {
        int posByte = pos / 8;
        int posBit = pos % 8;
        byte oldByte = data[posByte];
        oldByte = (byte) (((0xFF7F >> posBit) & oldByte) & 0x00FF);
        byte newByte = (byte) ((val << (8 - (posBit + 1))) | oldByte);
        data[posByte] = newByte;
    }

    public static void printBytes(byte[] data, String name) {
//       System.out.println("");
        System.out.println(name + ":");
        for (int i = 0; i < data.length; i++) {
            System.out.print(byteToBits(data[i]) + " ");
        }
//       System.out.println();
    }

    private static String byteToBits(byte b) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < 8; i++) {
            buf.append((int) (b >> (8 - (i + 1)) & 0x0001));
        }
        return buf.toString();
    }

    public static int uBToI(byte b) {
        return (int) b & 0xFF;
    }

    public static long uBToL(byte b) {
        return (long) b & 0xFF;
    }
    /**
     * table to convert a nibble to a hex char.
     */
    static char[] hexChar = {
        '0', '1', '2', '3',
        '4', '5', '6', '7',
        '8', '9', 'A', 'B',
        'C', 'D', 'E', 'F'};

    public static String toTelNumber(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        boolean Start = false;
        for (int i = 0; i < b.length; i++) {
            // look up high nibble char
            int Nib = (b[i] & 0xf0) >>> 4;
            if (!Start && Nib > 0) {
                Start = true;
            }
            if (Nib <= 9 && Start) {
                sb.append(hexChar[Nib]);
            }

            // look up low nibble char
            Nib = (b[i] & 0x0f);
            if (!Start && Nib > 0) {
                Start = true;
            }
            if (Nib <= 9 && Start) {
                sb.append(hexChar[Nib]);
            }
        }
        if (sb.length() == 0) {
            return null;
        }
        return "+" + sb.toString();
    }
}
