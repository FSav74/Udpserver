/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

import java.sql.Connection;

/**
 *
 * @author Luca
 */
public class TokenAuth {

    public long IDReservation;
    public long TokenHWID_N;
    public long Start;
    public long End;
    public int Status;

    public TokenAuth() {
        IDReservation = -1;
        TokenHWID_N = -1;
        Start = 0;
        End = 0;
        Status = -1;
    }

    public byte[] GetBytes() {
        byte[] Buff = new byte[13];
        Buff[0] = (byte) ((TokenHWID_N >> 24) & 0xFF);
        Buff[1] = (byte) ((TokenHWID_N >> 16) & 0xFF);
        Buff[2] = (byte) ((TokenHWID_N >> 8) & 0xFF);
        Buff[3] = (byte) (TokenHWID_N & 0xFF);

        Buff[4] = (byte) Status;
        Buff[5 + 0] = (byte) ((Start >> 24) & 0xFF);
        Buff[5 + 1] = (byte) ((Start >> 16) & 0xFF);
        Buff[5 + 2] = (byte) ((Start >> 8) & 0xFF);
        Buff[5 + 3] = (byte) (Start & 0xFF);
        Buff[9 + 0] = (byte) ((End >> 24) & 0xFF);
        Buff[9 + 1] = (byte) ((End >> 16) & 0xFF);
        Buff[9 + 2] = (byte) ((End >> 8) & 0xFF);
        Buff[9 + 3] = (byte) (End & 0xFF);

        return Buff;
    }

    public byte[] GetBytesFormatMifare() {
        byte[] Buff = new byte[17];
        Buff[0] = (byte) (this.TokenHWID_N >> 40 & 255);
        Buff[1] = (byte) (this.TokenHWID_N >> 32 & 255);
        Buff[2] = (byte) (this.TokenHWID_N >> 24 & 255);
        Buff[3] = (byte) (this.TokenHWID_N >> 16 & 255);
        Buff[4] = (byte) (this.TokenHWID_N >> 8 & 255);
        Buff[5] = (byte) (this.TokenHWID_N & 255);
        Buff[8] = (byte) this.Status;
        Buff[9] = (byte) (this.Start >> 24 & 255);
        Buff[10] = (byte) (this.Start >> 16 & 255);
        Buff[11] = (byte) (this.Start >> 8 & 255);
        Buff[12] = (byte) (this.Start & 255);
        Buff[13] = (byte) (this.End >> 24 & 255);
        Buff[14] = (byte) (this.End >> 16 & 255);
        Buff[15] = (byte) (this.End >> 8 & 255);
        Buff[16] = (byte) (this.End & 255);
        return Buff;
    }
}
