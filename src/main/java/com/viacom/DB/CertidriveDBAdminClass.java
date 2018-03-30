/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.DB;

import com.viacom.zbox.Token;
import com.viacom.zbox.zbox;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import zb_udp_server.ConfClass;
import zb_udp_server.LogsClass;
//import java.util.logging.Level;
//import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class CertidriveDBAdminClass {

    public Connection DbAdminConn = null;
    String userName = "viacom_db_user";
    String password = "viacom_db_user";
    String url = "jdbc:mysql://192.168.1.3/cddb_viacom_adm";
    private ConfClass Conf;
    private LogsClass Log;

    public boolean Init() {
        userName = Conf.CertidriveUserName;
        password = Conf.CertidrivePassword;
        url = Conf.CertidriveUrl + Conf.CertidriveAdminDBName;

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            DbAdminConn = DriverManager.getConnection(url, userName, password);
            Log.WriteLog(1, "Certidrive Database connection established");
//           System.out.println ("Database connection established");
        } catch (Exception e) {
//           System.err.println ("Cannot connect to database server: "+e.getMessage());
            Log.WriteLog(0, "Cannot connect to database server: " + e.getMessage());
            return false;
        }
//        finally
//        {
//            DeInit();
//        }
        return true;
    }

    /**
     * *
     * Imposta la classe di configurazione
     *
     * @param C1 : classe di configurazione
     * @return true se la riconfigurazinoe si è conclusa con successo
     */
    public boolean SetConf(ConfClass C1) {
        if (C1 != null) {
            Conf = C1;
            return true;
        } else {
            return false;
        }
    }

    public boolean SetLog(LogsClass L) {
        if (L != null) {
            Log = L;
            return true;
        } else {
            return false;
        }
    }

    public void DeInit() {
        if (DbAdminConn != null) {
            try {
                DbAdminConn.close();
                Log.WriteLog(1, "Certidrive Database connection terminated");
            } catch (SQLException ex) {
                Log.WriteEx(CertidriveDBAdminClass.class.getName(), ex);
            }
        }
    }

    /**
     * *
     * Apre la connessione al DB cliente
     *
     * @param IDAZ : ID dell'azienda da aprire
     * @return Struttura connection al DB cliente;
     * @throws Exception
     */
    Connection OpenAZDBConn(int IDAZ) throws Exception {
        //Verifico il nome del DB di destionazione sul DB di Amministrazione
        Statement statement;
        ResultSet rs;

        CheckConnection();

        String QueryString = "";
        try {
            statement = DbAdminConn.createStatement();

            QueryString = "SELECT IDAzienda,NomeDB from permessicd where IDAzienda=" + IDAZ;
            rs = statement.executeQuery(QueryString);
            if (!rs.next()) {
                throw (new Exception("IDAzienda inesistente"));
            }
            String ClientDBName = "cddb_" + rs.getString(2);
            rs.close();
            statement.close();

            // apro la connessione al DB client e comincio l'inserimento dei dati
            Connection AZDBConn;
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            AZDBConn = DriverManager.getConnection(Conf.CertidriveUrl + ClientDBName, userName, password);

            return AZDBConn;

        } catch (SQLException ex) {
            Log.WriteEx(CertidriveDBAdminClass.class.getName(), ex);
            Log.WriteLog(2, "Errore nella query: " + QueryString);
            throw (new Exception("CertidriveWebDBAdmin: Importazione fallita"));
        } catch (Exception ex) {
            Log.WriteLog(2, "Errore nella query: " + QueryString + " - " + ex.getMessage());
            throw (new Exception("CertidriveWebDBAdmin: Importazione fallita"));
        }


    }

    public void CheckConnection() {
        try {
            if (DbAdminConn.isValid(1000)) {
                return;
            }
        } catch (SQLException ex) {
        }
        Log.WriteLog(1, "Database connection TimedOut");
        DeInit();
        Init();
    }

    public boolean[] GetConducentiList(int IDAZ, Token TK) {
        PreparedStatement statement1;
        ResultSet rs;
        String QueryString;
        TK.SubNet = null;

        try {
            Connection CDDB = OpenAZDBConn(IDAZ);
            QueryString = "SELECT ID, CCODE, Subnet FROM conducenti WHERE CCODE=" + TK.TokenHWID_N;
            statement1 = CDDB.prepareStatement(QueryString);
            rs = statement1.executeQuery();
            if (rs.next()) {
                TK.SubNet = new boolean[16];
                int SN = rs.getInt("Subnet");
                TK.CertidriveIDAzienda = rs.getInt("ID");
                for (int i = 0; i < 16; i++) {
                    if ((SN & 0x1) == 1) {
                        TK.SubNet[i] = true;
                    }
                    SN = SN >> 1;
                }
            }
            CDDB.close();
        } catch (Exception ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return TK.SubNet;
    }

    public ArrayList<Integer> GetVeicoliListSubnet(int IDAZ, boolean Subnet[]) {
        PreparedStatement statement1;
        ResultSet rs;
        String QueryString;

        ArrayList<Integer> IDVeicoliList = new ArrayList<Integer>();

        try {
            Connection CDDB = OpenAZDBConn(IDAZ);
            QueryString = "SELECT ID, VCODE, Subnet FROM veicoli WHERE NOT Subnet=0";
            for (int i = 0; i < 16; i++) {
                if (Subnet[i]) {
                    QueryString += " AND ((SubNet&(1<<" + i + "))!=0)";
                }
            }
            statement1 = CDDB.prepareStatement(QueryString);
            rs = statement1.executeQuery();
            while (rs.next()) {
                IDVeicoliList.add(rs.getInt("ID"));
            }
            rs.close();
            statement1.close();
            CDDB.close();
        } catch (Exception ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return IDVeicoliList;
    }

    public boolean[] GetVeicoliList(int IDAZ, zbox ZB) {
        PreparedStatement statement1;
        ResultSet rs;
        String QueryString;
        ZB.SubNet = null;

        try {
            Connection CDDB = OpenAZDBConn(IDAZ);
            QueryString = "SELECT ID, VCODE, Subnet FROM veicoli WHERE VCODE=" + ZB.GetZBCod();
            statement1 = CDDB.prepareStatement(QueryString);
            rs = statement1.executeQuery();
            if (rs.next()) {
                ZB.SubNet = new boolean[16];
                int SN = rs.getInt("Subnet");
                ZB.CertidriveIDVeicolo = rs.getInt("ID");
                for (int i = 0; i < 16; i++) {
                    if ((SN & 0x1) == 1) {
                        ZB.SubNet[i] = true;
                    }
                    SN = SN >> 1;
                }
            }
            rs.close();
            statement1.close();
            CDDB.close();
        } catch (Exception ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ZB.SubNet;
    }

    public ArrayList<Integer> GetConducentiListSubnet(int IDAZ, boolean Subnet[]) {
        PreparedStatement statement1;
        ResultSet rs;
        String QueryString;

        ArrayList<Integer> IDConducentiList = new ArrayList<Integer>();

        try {
            Connection CDDB = OpenAZDBConn(IDAZ);
            QueryString = "SELECT ID, CCODE, Subnet FROM conducenti WHERE NOT Subnet=0";
            for (int i = 0; i < 16; i++) {
                if (Subnet[i]) {
                    QueryString += " AND ((SubNet&(1<<" + i + "))!=0)";
                }
            }
            statement1 = CDDB.prepareStatement(QueryString);
            rs = statement1.executeQuery();
            while (rs.next()) {
                IDConducentiList.add(rs.getInt("ID"));
            }
            rs.close();
            statement1.close();
            CDDB.close();
        } catch (Exception ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return IDConducentiList;
    }
}
