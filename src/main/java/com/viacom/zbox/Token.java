/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

import com.viacom.DB.DBAdminClass;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import zb_udp_server.DBConnector;

/**
 *
 * @author Luca
 */
public class Token {

    Connection DB;
    public long IDToken;
    public String Cognome;
    public String Nome;
    public int CDIDconducente;
    public int IDAzienda;
    public int Role;            // ruolo
                                // 0= Utente normale
                                // 1= Passpartout
                                // 2= Officina
    public boolean SubNet[];
    public int CertidriveIDAzienda;
    public long TokenHWID_N;
    public boolean isPasspartout;
    public boolean isPasspartoutActive;
    public long idreservation_passepartout;
    String NickName;

    public Token() {
        NickName = "Token";
        TokenHWID_N = -1;
        IDToken = -1;
        Role=-1;
        CertidriveIDAzienda = -1;
    }

    public Token(Connection LDB) {
        super();
        DB = LDB;
    }

    public void SetToken(String NickName, int TokenCod, Timestamp DT) {
    }

    public String GetTokenHexString() {
        String ret = null;
        if (TokenHWID_N >= 0) //ret = Long.toHexString(TokenHWID_N).toUpperCase();
        {
            ret = String.format("%08X", TokenHWID_N);
        }

        return ret;
    }

    public long SetTokenHexString(String HexString) {
        String Str = HexString.trim().toUpperCase();

        if (Str.length() != 8) {
            return -2;
        }
        for (int i = 0; i < 8; i++) {
            char c = Str.charAt(i);
            if ('0' <= c && c <= '9') {
                continue;
            } else if ('a' <= c && c <= 'f') {
                continue;
            } else if ('A' <= c && c <= 'F') {
                continue;
            } else {
                return -3;
            }
        }
        TokenHWID_N = Long.parseLong(Str, 16);

        return TokenHWID_N;
    }

    public long GetTokenHWID_N() {
        return TokenHWID_N;
    }

    public long SetTokenHWID_N(long LTokenHWID_N) {
        TokenHWID_N = LTokenHWID_N;
        return TokenHWID_N;
    }

    public boolean ReadToken() {
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            try {
                // esegue la query al DB
//                String QueryString = "SELECT * from Token WHERE TokenHWID_N=" + TokenHWID_N + " and IDAzienda=" + IDAzienda;
                String QueryString = "SELECT T.*,P.idreservation_passepartout, P.Active ActiveP from Token T left join reservation_passepartout P on T.IDToken=P.IDToken WHERE T.TokenHWID_N=" + this.TokenHWID_N + " and T.IDAzienda=" + this.IDAzienda;
                System.out.println(QueryString);
                statement = DB.prepareStatement(QueryString);
                rs = statement.executeQuery();

                if (rs.next()) {
                    this.IDToken = rs.getInt("IDToken");
                    this.Cognome = rs.getString("Cognome");
                    this.Nome = rs.getString("Nome");
                    this.CDIDconducente = rs.getInt("CertidriveIDConducente");
                    this.SetTokenHexString(rs.getString("TokenHWID"));
                    if (rs.getInt("idreservation_passepartout") > 0) {
                        this.isPasspartout = true;
                        this.idreservation_passepartout = rs.getInt("idreservation_passepartout");
                        this.isPasspartoutActive=(rs.getInt("ActiveP")>=1);
                    } else {
                        this.isPasspartout = false;
                        this.idreservation_passepartout = -1;
                        this.isPasspartoutActive=false;
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    public boolean ReadLongToken() {
        PreparedStatement statement = null;
        ResultSet rs = null;

        long Check_TokenHWID_N1 = this.TokenHWID_N & 0xFFFFFF00;
        long Check_TokenHWID_N2 = this.TokenHWID_N | 0xFF;
        try {
            try {
                String QueryString = "SELECT T.*,P.idreservation_passepartout, P.Active ActiveP from Token T left join reservation_passepartout P on T.IDToken=P.IDToken "
                        + "WHERE TokenHWID_N>=" + Check_TokenHWID_N1 + " and TokenHWID_N<=" + Check_TokenHWID_N2 + " and T.IDAzienda=" + this.IDAzienda;

                System.out.println(QueryString);
                statement = this.DB.prepareStatement(QueryString);
                rs = statement.executeQuery();

                if (rs.next()) {
                    this.IDToken = rs.getInt("IDToken");
                    this.Cognome = rs.getString("Cognome");
                    this.Nome = rs.getString("Nome");
                    this.CDIDconducente = rs.getInt("CertidriveIDConducente");
                    SetTokenHexString(rs.getString("TokenHWID"));
                    if (rs.getInt("idreservation_passepartout") > 0) {
                        this.isPasspartout = true;
                        this.idreservation_passepartout = rs.getInt("idreservation_passepartout");
                        this.isPasspartoutActive=(rs.getInt("ActiveP")>=1);
                    } else {
                        this.isPasspartout = false;
                        this.idreservation_passepartout = -1;
                        this.isPasspartoutActive=false;
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
}
