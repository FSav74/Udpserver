/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.DB;

import com.viacom.zbox.*;
import com.viacom.zbox.execeptions.ExceptionCRCError;
import com.viacom.zbox.execeptions.ExceptionInvalidRecordLenght;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bouncycastle.util.encoders.Hex;
import zb_udp_server.ConfClass;
import zb_udp_server.DBConnector;
import zb_udp_server.LogsClass;
import zb_udp_server.ZB_ElaboraDati;
import zb_udp_server.ZB_UDP_Record;
//import zb_udp_server.ZB_UDP_Comunication;

/**
 *
 * @author Luca
 */
public class DBAdminClass {

    public Connection DbAdminConn = null;
    public Connection DbRTConn = null;
    public Connection DBTaxi = null;
    private Connection CDDB = null;
    private CertidriveDBAdminClass CDDdAdminClass = null;
    String userName = "viacom_db_user";
    String password = "viacom_db_user";
    String url = "jdbc:mysql://192.168.1.237/blackbox_debug?autoReconnect=true";
    String driver = "com.mysql.jdbc.Driver";
    //String userName = null;
    //String password = null;
    //String url = "jdbc:derby://localhost:1527/SAFeListenerDB";
    //String url = "jdbc:derby:c:/DT/SAFeListenerDB";
    //String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    //String driver = "org.apache.derby.jdbc.ClientDriver";
    private ConfClass Conf;
    private LogsClass Log;

    public boolean Init() {
        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
        userName = Conf.userName;
        password = Conf.password;
        url = Conf.url + Conf.AdminDBName;

        try {
            Log.WriteLog(1, "DBAdminClass.Init(): start database connection. Please wait...");
            //Class.forName(driver).newInstance ();
            Log.WriteLog(1, "DBAdminClass.Init(): Class.forName (driver).newInstance ();");
            DbAdminConn = DriverManager.getConnection(url, userName, password);
            Log.WriteLog(1, "DBAdminClass.Init(): DbAdminConn = DriverManager.getConnection (url, userName, password);");
            DbRTConn = DriverManager.getConnection(url, userName, password);
            Log.WriteLog(1, "DBAdminClass.Init(): DbRTConn = DriverManager.getConnection (url, userName, password);");
            DbAdminConn.setAutoCommit(true);
            DbRTConn.setAutoCommit(true);

            DBConnector DBConn = DBConnector.getInstance();
            if (Conf.TaxiDataCheck > 0) {
                DBTaxi = DBConn.PoolTaxiDB.getConnection();
                DBTaxi.setAutoCommit(true);
            }

            Log.WriteLog(1, "Database connection established " + url);
        } catch (Exception e) {
            e.printStackTrace();
            Log.WriteLog(0, "Cannot connect to database server: " + e.getMessage());
            return false;
        }
        if (!Conf.CertidriveUrl.isEmpty()) {
            CDDdAdminClass = new CertidriveDBAdminClass();
            CDDdAdminClass.SetConf(Conf);
            CDDdAdminClass.SetLog(Log);
            CDDdAdminClass.Init();
        }
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

    public void CheckConnection() {
        try {
            DbRTConn.isValid(1000);
            if (DbAdminConn.isValid(1000)) {
                return;
            }
        } catch (SQLException ex) {
        }
        Log.WriteLog(1, "Database connection TimedOut");
        DeInit();
        Init();
    }

    public void DeInit() {
        if (DbAdminConn != null) {
            try {
                DbAdminConn.close();
                DbRTConn.close();
                Log.WriteLog(1, "Database connection terminated");
            } catch (SQLException ex) {
                Log.WriteEx(DBAdminClass.class.getName(), ex);
            }
        }
    }

    public ArrayList<zbox> GetZBoxList(int IDAzienda) {
        Statement statement;
        ResultSet rs;
        ArrayList<zbox> A;

        CheckConnection();

        try {
            if (DbAdminConn.isClosed()) {
                return null;
            }
        } catch (SQLException ex) {
            return null;
        }
        try {
            // esegue la query al DB
            statement = DbAdminConn.createStatement();

            String QueryString = "select IDBlackBox, BBSerial,BBSerial_N, Descr, Targa, NumTel, IDBlackBoxType, CertidriveIDVeicolo from BlackBox WHERE IDBlackBoxType=2";
            if (IDAzienda >= 0) {
                QueryString += " and IDAzienda=" + IDAzienda;
            }
            rs = statement.executeQuery(QueryString);
            A = new ArrayList<zbox>();
            while (rs.next()) {
                zbox ZB = new zbox();
                ZB.SerialN = Hex.decode(rs.getString("BBSerial"));
                ZB.SerialN_N = rs.getLong("BBSerial_N");
                ZB.NumTel = rs.getString("NumTel");
                ZB.Descr = rs.getString("Descr");
                ZB.IDAzienda = IDAzienda;
                ZB.CertidriveIDVeicolo = rs.getInt("CertidriveIDVeicolo");
                A.add(ZB);
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return A;
    }

    public Azienda GetAzienda(int IDAzienda) {
        Statement statement;
        ResultSet rs;
        Azienda A = null;

        CheckConnection();

        try {
            if (DbAdminConn.isClosed()) {
                return null;
            }
        } catch (SQLException ex) {
            return null;
        }
        try {
            // esegue la query al DB
            statement = DbAdminConn.createStatement();

            String QueryString = "Select IDAzienda,Denominazione, CodAzienda, CertidriveIDAzienda from Azienda "
                    + "WHERE IDAzienda=" + IDAzienda;
            rs = statement.executeQuery(QueryString);

            if (rs.next()) {
                A = new Azienda();
                A.IDAzienda = rs.getInt("IDAzienda");
                A.Denominazione = rs.getString("Denominazione");
                A.CodAz = rs.getString("CodAzienda");
                A.CertidriveIDAzienda = rs.getInt("CertidriveIDAzienda");
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return A;
    }

    public ArrayList<Azienda> GetAziendeList() {
        Statement statement;
        ResultSet rs;
        ArrayList<Azienda> A;

        CheckConnection();

        try {
            if (DbAdminConn.isClosed()) {
                return null;
            }
        } catch (SQLException ex) {
            return null;
        }
        try {
            // esegue la query al DB
            statement = DbAdminConn.createStatement();

            String QueryString = "Select IDAzienda,Denominazione, CodAzienda, CertidriveIDAzienda from Azienda";
            rs = statement.executeQuery(QueryString);
            A = new ArrayList<Azienda>();
            while (rs.next()) {
                Azienda Az = new Azienda();
                Az.IDAzienda = rs.getInt("IDAzienda");
                Az.Denominazione = rs.getString("Denominazione");
                Az.CodAz = rs.getString("CodAzienda");
                Az.CertidriveIDAzienda = rs.getInt("CertidriveIDAzienda");
                A.add(Az);
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return A;
    }

    /**
     * Ricerca nel DB la presenza della BB con il nome indicato in ZBoxSN Se la
     * trova restituisce la struttura ZBox
     *
     * @param ZBoxSN
     * @return
     */
    public zbox GetZBox(Connection Conn, byte[] ZBoxSN) {
        int i = -1;
        try {
            for (i = 0; i < Conf.CacheZBox.size(); i++) {
                if (java.util.Arrays.equals(Conf.CacheZBox.get(i).SerialN, ZBoxSN)) {
                    //System.out.println((new String(Hex.encode(ZBoxSN))).toUpperCase() + " found in cache");
                    return Conf.CacheZBox.get(i);
                }
            }
        } catch (java.lang.NullPointerException Ex) {
            System.out.println("errore GetZBox SN" + (new String(Hex.encode(ZBoxSN))).toUpperCase() + " I=" + i + " Conf.CacheZBox.size()=" + Conf.CacheZBox.size());
        }
        PreparedStatement statement;
        ResultSet rs;

//        CheckConnection();
//        try {
//            if (DbAdminConn.isClosed()) {
//                return null;
//            }
//        } catch (SQLException ex) {
//            return null;
//        }
        try {
            // esegue la query al DB
            zbox ZB = null;

            String QueryString = "select IDBlackBox, BBSerial,BBSerial_N, Descr, Targa, NumTel, IDBlackBoxType ,"
                    + " AESRootKey, AESKeyIn, AESKeyOut, IP, Port, ProtV, StatoConnessione, IDAzienda, CertidriveIDVeicolo,IDSWVersion,"
                    + " AutoAccFileDownload"
                    + " from BlackBox \n\r"
                    + " WHERE BBSerial='" + (new String(Hex.encode(ZBoxSN))).toUpperCase() + "';";

//            String QueryString = "select IDBlackBox, BBSerial, Descr, Targa, NumTel, IDBlackBoxType from BlackBox \n\r"+
//                    "WHERE BBSerial='"+ZBoxSN+"';";
//            System.out.println(QueryString);
            statement = Conn.prepareStatement(QueryString);
            rs = statement.executeQuery(QueryString);
            if (rs.next()) {
                ZB = new zbox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
                ZB.SerialN = Hex.decode(rs.getString("BBSerial"));
                ZB.SerialN_N = rs.getLong("BBSerial_N");
                ZB.Targa = rs.getString("Targa");

                ZB.NumTel = rs.getString("NumTel");
                ZB.Descr = rs.getString("Descr");
                ZB.AESRootKey[0] = rs.getBytes("AESRootKey");
                ZB.AESKeyIn = rs.getBytes("AESKeyIn");
                ZB.AESKeyOut = rs.getBytes("AESKeyOut");
                ZB.IDAzienda = rs.getInt("IDAzienda");
                ZB.CertidriveIDVeicolo = rs.getInt("CertidriveIDVeicolo");
                ZB.IDSWVersion = rs.getInt("IDSWVersion");
                ZB.AutoAccFileDownload=rs.getInt("AutoAccFileDownload");

                try {
                    ZB.IP = InetAddress.getByName(rs.getString("IP"));
                } catch (UnknownHostException e) {
                    ZB.IP = null;
                }
                ZB.Port = rs.getInt("Port");
                ZB.ProtV = rs.getInt("ProtV");
                ZB.StatoConnessione = rs.getInt("StatoConnessione");
                if (ZB.AESRootKey[0] == null) {
                    ZB.AESRootKey[0] = new byte[16];
                    java.util.Arrays.fill(ZB.AESRootKey[0], (byte) 0);
                }
                if (ZB.AESKeyIn == null) {
                    ZB.AESKeyIn = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyIn, (byte) 0);
                }
                if (ZB.AESKeyOut == null) {
                    ZB.AESKeyOut = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyOut, (byte) 0);
                }
                Conf.CacheZBox.add(ZB);
            } else {
                Log.WriteLog(1, "ZBox non esistente nel DB\n\r" + QueryString);
            }

            rs.close();
            statement.close();
            return ZB;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    /**
     * Ricerca nel DB la presenza della BB con il nome indicato in ZBoxSN Se la
     * trova restituisce la struttura ZBox
     *
     * @param ZBoxSN
     * @return
     */
    public zbox GetZBox(int IDBlackBox) {
        PreparedStatement statement;
        ResultSet rs;

        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return null;
            }
        } catch (SQLException ex) {
            return null;
        }
        try {
            // esegue la query al DB
            zbox ZB = null;

            String QueryString = "select IDBlackBox, BBSerial,BBSerial_N, Descr, Targa, NumTel, IDBlackBoxType ,"
                    + " AESRootKey, AESKeyIn, AESKeyOut, IP, Port, ProtV, StatoConnessione, IDAzienda, CertidriveIDVeicolo"
                    + " from BlackBox \n\r"
                    + " WHERE IDBlackBox=" + IDBlackBox + ";";

            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery(QueryString);
            if (rs.next()) {
                if (rs.getInt("IDBlackBoxType") != 2) {
                    return null;
                }
                ZB = new zbox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
                try {
                    ZB.SerialN = Hex.decode(rs.getString("BBSerial"));
                } catch (java.lang.StringIndexOutOfBoundsException Ex) {
                }
                ZB.SerialN_N = rs.getLong("BBSerial_N");
                ZB.Targa = rs.getString("Targa");

                ZB.NumTel = rs.getString("NumTel");
                ZB.Descr = rs.getString("Descr");
                ZB.AESRootKey[0] = rs.getBytes("AESRootKey");
                ZB.AESKeyIn = rs.getBytes("AESKeyIn");
                ZB.AESKeyOut = rs.getBytes("AESKeyOut");
                ZB.IDAzienda = rs.getInt("IDAzienda");

                try {
                    ZB.IP = InetAddress.getByName(rs.getString("IP"));
                } catch (UnknownHostException e) {
                    ZB.IP = null;
                }
                ZB.Port = rs.getInt("Port");
                ZB.ProtV = rs.getInt("ProtV");
                ZB.StatoConnessione = rs.getInt("StatoConnessione");
                ZB.CertidriveIDVeicolo = rs.getInt("CertidriveIDVeicolo");
                if (ZB.AESRootKey[0] == null) {
                    ZB.AESRootKey[0] = new byte[16];
                    java.util.Arrays.fill(ZB.AESRootKey[0], (byte) 0);
                }
                if (ZB.AESKeyIn == null) {
                    ZB.AESKeyIn = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyIn, (byte) 0);
                }
                if (ZB.AESKeyOut == null) {
                    ZB.AESKeyOut = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyOut, (byte) 0);
                }
            }
            rs.close();
            statement.close();
            return ZB;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    /**
     * Ricerca nel DB la presenza della BB con il nome indicato in ZBoxSN Se la
     * trova restituisce la struttura ZBox
     *
     * @param ZBoxSN
     * @return
     */
    public boolean SetZBox(Connection Conn, zbox ZB) {
        PreparedStatement statement;

//        CheckConnection();

//        try {
//            if (DbAdminConn.isClosed()) {
//                return false;
//            }
//        } catch (SQLException ex) {
//            return false;
//        }
        try {
            zbox checkZB = GetZBox(Conn, ZB.SerialN);
            String QueryString;
            Timestamp Now = new Timestamp((new java.util.Date()).getTime());

            if (checkZB == null) { //insert
                QueryString = "INSERT INTO BlackBox (IDBlackBoxType, IDAzienda, BBSerial, CertSerialNum,"
                        + "Descr, NumTel, Targa, AESKeyIn,AESKeyOut,AESRootKey,IP, Port, ProtV, PackNRef, StatoConnessione, "
                        + "LastContact,BBSerial_N)"
                        + " VALUES(2,11,?,0,"
                        + "?,?,?,?,?,?,?,?,?,?,?, ?,?)";
                statement = Conn.prepareStatement(QueryString);
                statement.setString(1, (new String(Hex.encode(ZB.SerialN))).toUpperCase());
                statement.setString(2, ZB.Descr);
                statement.setString(3, ZB.NumTel);
                statement.setString(4, ZB.Targa);
                statement.setBytes(5, ZB.AESKeyIn);
                statement.setBytes(6, ZB.AESKeyOut);
                statement.setBytes(7, ZB.AESRootKey[0]);
                statement.setString(8, ZB.IP.getHostAddress());
                statement.setInt(9, ZB.Port);
                statement.setInt(10, ZB.ProtV);
                statement.setInt(11, ZB.PackNRef);
                statement.setInt(12, ZB.StatoConnessione);
                statement.setTimestamp(13, Now);
                statement.setLong(14, ZB.SerialN_N);
            } else { // update            
                QueryString = "UPDATE BlackBox SET "//Descr=?, "//NumTel=?, Targa=?, "
                        + "AESKeyIn=?,AESKeyOut=?,AESRootKey=?,IP=?, Port=?, ProtV=? , PackNRef=?, StatoConnessione=? , LastContact=?"
                        + "WHERE IDBlackBox=? ";
                statement = Conn.prepareStatement(QueryString);
//                statement.setString(1, ZB.Descr);
//                statement.setString(2, ZB.NumTel);
//                statement.setString(3, ZB.Targa);
                statement.setBytes(1, ZB.AESKeyIn);
                statement.setBytes(2, ZB.AESKeyOut);
                statement.setBytes(3, ZB.AESRootKey[0]);
                statement.setString(4, ZB.IP.getHostAddress());
                statement.setInt(5, ZB.Port);
                statement.setInt(6, ZB.ProtV);
                statement.setInt(7, ZB.PackNRef);
                statement.setInt(8, ZB.StatoConnessione);
                statement.setTimestamp(9, Now);
                statement.setInt(10, checkZB.IDBlackBox);
            }

            // esegue la query al DB
//            System.out.println(QueryString);
            statement.execute();

            statement.close();
            return true;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
             Log.WriteLog(1, "setZbox: "+ex.toString());
             Log.WriteLog(1, "setZbox Stacktrace :"+Utils.getStackTrace(ex));
            return false;
        }
    }

    public int GetNextID(String TableName, String FieldName) {
        int ID = 0;
        try {
            PreparedStatement statement;
            ResultSet rs;

            String Query = "select MAX(" + FieldName + ") ID from APP." + TableName;
            statement = DbAdminConn.prepareStatement(Query);

            rs = statement.executeQuery();
            if (rs.next()) {
                ID = rs.getInt("ID") + 1;
            }
            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }

        return ID;
    }

    public int FindWaitingTrackingReq() {
        int ID = -1;
        try {
            PreparedStatement statement;
            ResultSet rs;

            String Query = "select * from APP.TRACKING where Inizio IS NULL AND Fine IS NULL AND Tipo=1";
            statement = DbAdminConn.prepareStatement(Query);

            rs = statement.executeQuery();
            if (rs.next()) {
                ID = rs.getInt("IDTrack");
            }
            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }

        return ID;
    }

    public int FindWaitingLocReq() {
        int ID = -1;
        try {
            PreparedStatement statement;
            ResultSet rs;

            String Query = "select * from APP.TRACKING where Inizio IS NULL AND Fine IS NULL AND Tipo=2";
            statement = DbAdminConn.prepareStatement(Query);

            rs = statement.executeQuery();
            if (rs.next()) {
                Log.WriteLog(1, "Richiesta TrackLoc ricevuta");
                ID = rs.getInt("IDTrack");
            }
            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }

        return ID;
    }

    /**
     * Inserisce un record nel DB se non presente
     *
     * @param ZB
     * @param rec
     * @return la struttura ZB_UDP_Record popolata con il campo binario del
     * record nel campo Rec e il campo IDRec popolato come segue 0 : se la
     * memorizzazione non è avvenuta con successo -1 : se la memorizzazione è
     * corretta ( ma non è stato generato il IDRec) -2 : se il record era gia'
     * presente >0 : se è stato generato un IDRec valido e l'inserimento si è
     * concluso correttamente
     */
    public ZB_UDP_Record AddRecord(Connection DbRTConn, zbox ZB, byte rec[], boolean CheckBefore) {
        
        ZB_UDP_Record Ret = new ZB_UDP_Record();
        Ret.IDRec = 0;

        System.arraycopy(rec, 0, Ret.Rec, 0, 48);
        
        if (Conf.DisableDBWriteData == 1) {
            Ret.IDRec = 1;
            return Ret;
        }
        PreparedStatement statement=null;
        ResultSet rs=null;
//        CheckConnection();
//        try {
//            if (DbRTConn.isClosed()) {  return Ret;  }
//        } catch (SQLException ex) {     return Ret;  }
        java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
        String Stato = "stato 0";


        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString;
            if (CheckBefore) {
                // esegue la query al DB
                Stato = "stato 1";
                QueryString = "select IDBlackBox, Record FROM ZBRecords \n\r"
                        + " WHERE IDBlackBox=? and Record=?";
                //                    + " WHERE IDBlackBox=? and Record=? and Time>?";
                try {
                    statement = DbRTConn.prepareStatement(QueryString);
                    statement.setInt(1, ZB.IDBlackBox);
                    statement.setBytes(2, rec);
                    //            statement.setTimestamp(3,new Timestamp((new java.util.Date()).getTime()-(30*24*60*60*1000)));
                    rs = statement.executeQuery();
                    Stato = "stato 2";

                    if (rs.next()) {
                        //System.out.println("Record gia' presente");
                        Ret.IDRec = -2;
//                        DbRTConn.rollback();
                        return Ret;
                    }
                } finally {
                    if (rs != null) {rs.close();}
                    if (statement != null) {statement.close();}
                }
            }
            java.sql.Timestamp now2 = new java.sql.Timestamp((new java.util.Date()).getTime());
            float TimeDiff1 = ((float) (now1.getTime() - now2.getTime())) / 1000;

            Stato = "stato 3 TimeDiff1=" + TimeDiff1;
            QueryString = "INSERT INTO ZBRecords (IDBlackBox, Record, Stato, Time)"
                    + " VALUES(?,?,0,?)";
            try {
            statement = DbRTConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setBytes(2, rec);
            statement.setTimestamp(3, new Timestamp((new java.util.Date()).getTime()));
            statement.execute();
            Stato = "stato 4";
            Ret.IDRec = -1;
            ResultSet generatedKeys;
            generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                Ret.IDRec = generatedKeys.getLong(1);
                generatedKeys.close();
            }
            } finally {
                if (statement !=null) {statement.close();}
            }
//            DbRTConn.commit();
            return Ret;
        } catch (SQLException ex) {
            java.sql.Timestamp now2 = new java.sql.Timestamp((new java.util.Date()).getTime());

            float TimeDiff2 = ((float) (now1.getTime() - now2.getTime())) / 1000;
            Log.WriteLog(3, "AddRecord Select Stato=" + Stato + " TimeDiff2=" + TimeDiff2);
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            try {
//                DbRTConn.rollback();
//            } catch (SQLException ex1) {
//                Log.WriteEx(DBAdminClass.class.getName(), ex);
//                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
//            }
            return Ret;
        }

    }

    /**
     * Leggi il valore del ChunkNum dalla tabella zbfileupload del DB
     *
     * @param ZB
     * @param rec
     * @return
     */
    public int ReadChunkNumFromZBFileUpload(zbox ZB, long IdFile) {
        PreparedStatement statement;
        ResultSet rs;
        int ChunkNum = 0;
        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "select idZBFileUpload, LastChunk"
                    + " from ZBFileUpload"
                    + " WHERE IDBlackBox=? and idZBFileUpload=? and Stato=0";

            statement = DbRTConn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();

            if (rs.next()) {
                ChunkNum = rs.getInt("LastChunk");
            }
            Log.WriteLog(3, "ZB.IDBlackBox=" + ZB.IDBlackBox + " IdFile=" + IdFile + " ChunkNum:" + ChunkNum);
            rs.close();
            statement.close();
//            DbRTConn.commit();
            return ChunkNum;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            /*            try {
             DbRTConn.rollback();
             } catch (SQLException ex1) {
             Log.WriteEx(DBAdminClass.class.getName(), ex);
             Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
             } */
            return -1;
        }

    }

    /**
     * Leggi il valore del ChunkTot dalla tabella zbfileupload del DB
     *
     * @param ZB
     * @param rec
     * @return
     */
    public int ReadChunkTotFromZBFileUpload(zbox ZB, long IdFile) {
        PreparedStatement statement;
        ResultSet rs;
        int ChunkTot = 0;
        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "select idZBFileUpload, ChunkTot"
                    + " from ZBFileUpload"
                    + " WHERE IDBlackBox=? and idZBFileUpload=? and Stato=0";

            statement = DbRTConn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();

            if (rs.next()) {
                ChunkTot = rs.getInt("ChunkTot");
            }
            Log.WriteLog(3, "ZB.IDBlackBox=" + ZB.IDBlackBox + " IdFile=" + IdFile + " ChunkTot:" + ChunkTot);
            rs.close();
            statement.close();
//            DbRTConn.commit();
            return ChunkTot;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            /*            try {
             DbRTConn.rollback();
             } catch (SQLException ex1) {
             Log.WriteEx(DBAdminClass.class.getName(), ex);
             Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
             }*/
            return -1;
        }

    }

    /**
     * Aggiorna il valore dello Stato nella tabella zbfileupload del DB
     *
     * @param ZB
     * @param rec
     * @return
     */
    public boolean UpdateStatoIntoZBFileUpload(zbox ZB, long IdFile) {
        PreparedStatement statement;
        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return false;
            }
        } catch (SQLException ex) {
            return false;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "UPDATE ZBFileUpload SET Stato=1"
                    + " WHERE IDBlackBox=? and idZBFileUpload=? ";

            statement = DbRTConn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            statement.execute();
            statement.close();
//            DbRTConn.commit();
            return true;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            try {
                DbRTConn.rollback();
            } catch (SQLException ex1) {
                Log.WriteEx(DBAdminClass.class.getName(), ex);
                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
            }
            return false;
        }
    }

    /**
     * Inserisce il record di FileUploadStart nella tabella zbfileupload del DB
     *
     * @param ZB
     * @param rec
     * @return
     */
    public long AddStartToZBFileUpload(zbox ZB, int filetype, int ChunkTot, int ChunkSize, String filename, byte CRC[]) {
        PreparedStatement statement;
        ResultSet rs;
        long ret = -1;
        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return ret;
            }
        } catch (SQLException ex) {
            return ret;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            // esegue la query al DB
            String QueryString = "select idZBFileUpload, IDBlackBox, FileTimestamp, Filename"
                    + " from ZBFileUpload "
                    + " WHERE IDBlackBox=? and Filename=? AND FileTimestamp>?";

            statement = DbRTConn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setString(2, filename);
            statement.setTimestamp(3, new Timestamp((new java.util.Date()).getTime() - 604800000));  // 7 giorni
            rs = statement.executeQuery();

            if (rs.next()) {
                try {
                    ret = rs.getLong("idZBFileUpload");
                    System.out.println("Record gia' presente");
                    rs.close();
                    statement.close();
                    QueryString = "UPDATE ZBFileUpload SET Stato=0, Size=0, LastChunk=0,  ChunkTot=?, "
                            + " FileType=? , Hash=?, FileData=? ,FileTimestamp=?, ChunkSize=?"
                            + " WHERE idZBFileUpload=?";

                    statement = DbRTConn.prepareStatement(QueryString);
                    statement.setInt(1, ChunkTot);
                    statement.setInt(2, filetype);
                    statement.setBytes(3, CRC);
                    byte zero[] = new byte[ChunkTot * ChunkSize];
                    statement.setBytes(4, zero);
                    statement.setTimestamp(5, new Timestamp((new java.util.Date()).getTime()));
                    statement.setInt(6, ChunkSize);
                    statement.setLong(7, ret);
                    statement.execute();
    //                statement.close();
    //                rs.close();
    //                statement.close();

                    return ret;
                } finally {
                    if (rs != null) { rs.close();}
                    if (statement!= null) { statement.close();}
                }
            }
            rs.close();
            statement.close();

            try {
                QueryString = "INSERT INTO ZBFileUpload (IDBlackBox, FileTimestamp, FileType, Size, Stato, LastChunk, ChunkTot, ChunkSize, Filename, Hash, FileData)"
                        + " VALUES(?,?,?,0,0,0,?,?,?,?,?)";

                statement = DbRTConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                statement.setInt(1, ZB.IDBlackBox);
                statement.setTimestamp(2, new Timestamp((new java.util.Date()).getTime()));
                statement.setInt(3, filetype);
                statement.setInt(4, ChunkTot);
                statement.setInt(5, ChunkSize);
                statement.setString(6, filename);
                statement.setBytes(7, CRC);
                byte zero[] = new byte[ChunkTot * ChunkSize];
                statement.setBytes(8, zero);
                statement.execute();
                ResultSet generatedKeys;
                generatedKeys = statement.getGeneratedKeys();
                if (generatedKeys.next()) {
                    ret = generatedKeys.getLong(1);
                    generatedKeys.close();
                }
            } finally {
                if( statement != null) { statement.close();}
            }
//            DbRTConn.commit();
            return ret;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            try {
//                DbRTConn.rollback();
//            } catch (SQLException ex1) {
//                Log.WriteEx(DBAdminClass.class.getName(), ex);
//                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
//            }
                    Log.WriteLog(1, "AddStartToZBFileUpload: "+ex.toString());
                    Log.WriteLog(1, "AddStartToZBFileUpload Stacktrace :"+Utils.getStackTrace(ex));
            return ret;
        }
    }

    /**
     * Legge le informazioni di download dalla tabella zbfiledownload del DB
     *
     * @param ZB
     * @return
     */
    public DownloadInfo ReadDownloadInfo(zbox ZB) {
        PreparedStatement statement;
        ResultSet rs;
        DownloadInfo DwnlInfo = null;
        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return DwnlInfo;
            }
        } catch (SQLException ex) {
            return DwnlInfo;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            // esegue la query al DB
            String QueryString = "SELECT DS.idzbfiledownloadsession, DS.FilePathOnZBox, FD.FileDownloadSize,  "
                    + "FD.FileDownloadType from zbfiledownloadsession DS "
                    + "LEFT JOIN blackbox BB ON BB.IdBlackBox=DS.IdBlackBox "
                    + "LEFT JOIN zbfiledownload FD ON FD.Idzbfiledownload=DS.Idzbfiledownload "
                    + "WHERE DS.Stato=0 and BB.IdBlackBox=? "
                    + "ORDER BY DS.idzbfiledownloadsession Desc";

            statement = DbRTConn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                DwnlInfo = new DownloadInfo();
                DwnlInfo.FileType = rs.getInt("FD.FileDownloadType");
                DwnlInfo.FileSize = rs.getLong("FD.FileDownloadSize");
                DwnlInfo.PathFileName = rs.getString("DS.FilePathOnZBox");
                DwnlInfo.IDFile = rs.getLong("DS.idzbfiledownloadsession");
//                DbRTConn.commit();
            }
            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            
            
            
            /*            try {
             DbRTConn.rollback();
             } catch (SQLException ex1) {
             Log.WriteEx(DBAdminClass.class.getName(), ex);
             Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
             }*/
        }
        return DwnlInfo;
    }

    /**
     * Legge le informazioni dalla tabella zbfiledownload del DB per l'invio di
     * un ChunkData
     *
     * @param ZB
     * @param FT
     * @return DownloadChunkData
     */
    public DownloadChunkData ReadDownloadChunkData(zbox ZB, FileTrasfer FT) {
        PreparedStatement statement;
        ResultSet rs;
        DownloadChunkData DwnlChkData = null;
        byte ByteData[];
        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return DwnlChkData;
            }
        } catch (SQLException ex) {
            return DwnlChkData;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            //TODO: Per forza doppia query? Sembra di no, prendo il dato passato
            //Leggo l'ultimo chunck inviato e che è stato aggiornato in UpdateDataToZBFileDownloadSession 
            /*
             String QueryStringLastChunk = "SELECT DS.LastSentChunk from zbfiledownloadsession DS "
             + "LEFT JOIN blackbox BB ON BB.IdBlackBox=DS.IdBlackBox "
             + "LEFT JOIN zbfiledownload FD ON FD.Idzbfiledownload=DS.Idzbfiledownload "
             + "WHERE DS.Stato=0 and BB.IdBlackBox=? "
             + "ORDER BY DS.idzbfiledownloadsession ";
            
             statement = DbRTConn.prepareStatement(QueryStringLastChunk);
             statement.setInt(1, ZB.IDBlackBox);
            
             rs = statement.executeQuery();
             if (rs.next()) {
             //Salva ultimo chunk letto
             int LastChunck = rs.getInt("DS.LastSentChunk");
             }
            
             rs.close();
             statement.close();
             */

            //ATTENZIONE: FT.ChunkTot non è popolato il pacchetto FileChunckReq non ha ChunckTot
            String QueryString;
            //Calcola il residuo per la query
//            Log.WriteLog(1,"ChunckCurr/ChunckTot: "+FT.ChunkCurr+"/"+FT.ChunkTot);
            //Mi trovo nell'ultimo Chunck?

            //Capisco prima quale ultimo chunk è stato inviato           
            //esegue la query al DB
            //ESP: la query è stata modificata in modo da ritornare ChunckCurr da FileTransfer che mi fornisce la chiamata della ZBOX 
            QueryString = "SELECT DS.idzbfiledownloadsession, DS.LastSentChunk, FD.FileDownloadSize, ceil(FD.FileDownloadSize/" + FT.ChunkSize + ") TotChunk,"
                    + "SUBSTRING(FD.FileDownloadContent," + Integer.toString((FT.ChunkCurr * FT.ChunkSize) + 1)
                    + ", IF (((" + FT.ChunkCurr + "+1)*" + FT.ChunkSize + ")>FD.FileDownloadSize, FD.FileDownloadSize MOD " + FT.ChunkSize + ", " + FT.ChunkSize + ")) DataChunk from zbfiledownloadsession DS "
                    + "LEFT JOIN blackbox BB ON BB.IdBlackBox=DS.IdBlackBox "
                    + "LEFT JOIN zbfiledownload FD ON FD.Idzbfiledownload=DS.Idzbfiledownload "
                    + "WHERE DS.Stato=0 and BB.IdBlackBox=? "
                    + "ORDER BY DS.idzbfiledownloadsession Desc";

            statement = DbRTConn.prepareStatement(QueryString);
            //Utilizzare il set statement
            //statement.setInt(1, FT.ChunkCurr);
            statement.setInt(1, ZB.IDBlackBox);
            //Log Query
            Log.WriteLog(5, "Query for ChunckCurr: " + QueryString + " for BlackBox: " + Integer.toString(ZB.IDBlackBox));
            //Send Query
            rs = statement.executeQuery();
            if (rs.next()) {
                DwnlChkData = new DownloadChunkData();
                DwnlChkData.IDFile = rs.getLong("DS.idzbfiledownloadsession");
//                DwnlChkData.ChunkNum = rs.getInt("DS.LastSentChunk");
                DwnlChkData.ChunkNum = FT.ChunkCurr;
                FT.ChunkTot = rs.getInt("TotChunk");
                if (FT.ChunkCurr > FT.ChunkTot) {
                    Log.WriteLog(1, "Errore: IDBB " + Integer.toString(ZB.IDBlackBox) + " Chunk " + FT.ChunkCurr + "/" + FT.ChunkTot);
                    return null;  // pacchetto richiesto non valido
                }
                int SizeTot = rs.getInt("FD.FileDownloadSize");
                //ByteData = rs.getBytes("SUBSTRING(FD.FileDownloadContent,"+ Integer.toString((FT.ChunkCurr)+1) +",1000)");
                ByteData = rs.getBytes("DataChunk"); //Prendo la quarta colonna ovvero il risultato del quarto campo della SELECT precedente
                //Log
                Log.WriteLog(1, "Transfer ChunkNumber: " + Integer.toString(FT.ChunkCurr));

                //Calcola il residuo
                if ((FT.ChunkSize * (DwnlChkData.ChunkNum + 1)) > SizeTot) {
                    DwnlChkData.ByteCount = (SizeTot % FT.ChunkSize);
                } else {
                    DwnlChkData.ByteCount = FT.ChunkSize;
                }

                DwnlChkData.ByteCount = ByteData.length;

                Log.WriteLog(1, "ChunckCurr/ChunckTot: " + FT.ChunkCurr + "/" + FT.ChunkTot + " IDZB:" + ZB.IDBlackBox);
                Log.WriteLog(1, "DwnlChkData.ByteCount: " + DwnlChkData.ByteCount);
                Log.WriteLog(1, "ByteData Lenght: " + ByteData.length);
                Log.WriteLog(1, "DwnlChkData.ChunkNum: " + DwnlChkData.ChunkNum);
                Log.WriteLog(5, "ByteArray ByteData: " + Arrays.toString(ByteData));
                //System.arraycopy(ByteData, 1000*DwnlChkData.ChunkNum, DwnlChkData.ChunkData, 0, DwnlChkData.ByteCount);
                //ESP 5_06_2013 (Trasferisco sempre 1000 byte, tranne nell'ultimo chunck dove trasferisco il residuo)
                //ES. 352.644 = 352 * 1000 + 644
                //Quindi vengono spediti 644 byte con padding zero dei dati non necessari
                System.arraycopy(ByteData, 0, DwnlChkData.ChunkData, 0, ByteData.length);
                rs.close();
                statement.close();
//                DbRTConn.commit();
            }
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            /*            try {
             DbRTConn.rollback();
             } catch (SQLException ex1) {
             Log.WriteEx(DBAdminClass.class.getName(), ex);
             Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
             }*/
        }
        return DwnlChkData;
    }

    /**
     * Aggiorna il chunk data relativo ad un file di upload nella tabella
     * zbfileupload del DB
     *
     * @param ZB
     * @param rec
     * @return
     */
    public FileTrasfer UpdateChunkDataToZBFileUpload(zbox ZB, int ByteCount, int ChunkNum, long IdFile, byte chunk[]) {
        PreparedStatement statement;
        ResultSet rs;
        int SizeCurr;
        byte ByteFileData[];
        byte ByteFileDataDest[];
        FileTrasfer FT = null;

        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return FT;
            }
        } catch (SQLException ex) {
            return FT;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "select idZBFileUpload, ChunkTot, ChunkSize, LastChunk, IDBlackBox, FileData"
                    + " from ZBFileUpload "
                    + " WHERE IDBlackBox=? and idZBFileUpload=?";

            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();
            if (rs.next()) {
                if (chunk != null) {
                    System.out.append("Chunk Size=" + chunk.length + " ChunkNum" + ChunkNum+" ");
                }

                FT = new FileTrasfer();
                FT.ChunkTot = rs.getInt("ChunkTot");
                FT.IDFile = IdFile;
                FT.ChunkSize = rs.getInt("ChunkSize");
                SizeCurr = (FT.ChunkSize * ChunkNum) + ByteCount;
                ByteFileData = rs.getBytes("FileData");
                ByteFileDataDest = new byte[SizeCurr];
                if (ByteFileData != null) {
                    System.out.append("ByteFileData Size=" + ByteFileData.length);
                }
                System.arraycopy(ByteFileData, 0, ByteFileDataDest, 0, FT.ChunkSize * ChunkNum);
                System.arraycopy(chunk, 0, ByteFileDataDest, FT.ChunkSize * ChunkNum, ByteCount);

                rs.close();
                statement.close();

                QueryString = "UPDATE ZBFileUpload SET Size=?, LastChunk=?, FileData=? "
                        + " WHERE IDBlackBox=? and idZBFileUpload=?";

                statement = DbRTConn.prepareStatement(QueryString);
                statement.setInt(1, SizeCurr);
                statement.setInt(2, ChunkNum);
                statement.setBytes(3, ByteFileDataDest);
                statement.setInt(4, ZB.IDBlackBox);
                statement.setLong(5, IdFile);
                statement.execute();
                statement.close();
//                DbRTConn.commit();
            }
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            try {
//                DbRTConn.rollback();
//            } catch (SQLException ex1) {
//                Log.WriteEx(DBAdminClass.class.getName(), ex);
//                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
//            }

        }
        return FT;
    }

    /**
     * Aggiorna i dati nella tabella delle sessioni di trasferimento di un file
     * in download
     *
     * @param ZB
     * @param rec
     * @return
     */
    public void UpdateDataToZBFileDownloadSession(zbox ZB, int ChunkNum, long IdFile, int Stato) {
        PreparedStatement statement;

        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return;
            }
        } catch (SQLException ex) {
            return;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "UPDATE ZBFileDownloadSession SET Stato=?, LastSentChunk=?, LastSent_Date=? "
                    + " WHERE IDBlackBox=? and idZBFileDownloadSession=?";

            statement = DbRTConn.prepareStatement(QueryString);
            statement.setInt(1, Stato);
            statement.setInt(2, ChunkNum);
            statement.setTimestamp(3, new Timestamp(new java.util.Date().getTime()));
            statement.setInt(4, ZB.IDBlackBox);
            statement.setLong(5, IdFile);
            Log.WriteLog(3, "Update Chunk numero: " + ChunkNum + " IdFile: " + IdFile);
            statement.execute();
            statement.close();
//            DbRTConn.commit();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            try {
//                DbRTConn.rollback();
//            } catch (SQLException ex1) {
//                Log.WriteEx(DBAdminClass.class.getName(), ex);
//                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
//            }
        }
    }

    /**
     * Conclude l'aggiornamento dei dati nella tabella delle sessioni di
     * trasferimento di un file in download
     *
     * @param ZB
     * @param rec
     * @return
     */
    public void FinalUpdateDataToZBFileDownloadSession(zbox ZB, long IdFile, int Stato) {
        PreparedStatement statement;

        CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return;
            }
        } catch (SQLException ex) {
            return;
        }
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "UPDATE ZBFileDownloadSession SET Stato=? "
                    + " WHERE IDBlackBox=? and idZBFileDownloadSession=?";

            statement = DbRTConn.prepareStatement(QueryString);
            statement.setInt(1, Stato);
            statement.setInt(2, ZB.IDBlackBox);
            statement.setLong(3, IdFile);
            statement.execute();
            statement.close();
//            DbRTConn.commit();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            try {
//                DbRTConn.rollback();
//            } catch (SQLException ex1) {
//                Log.WriteEx(DBAdminClass.class.getName(), ex);
//                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
//            }
        }
    }

    /**
     * Inserisce un record nel DB se non presente
     *
     * @param ZB
     * @param rec
     * @return
     */
    public int ReadRecord() {
        PreparedStatement statement, statement1;
        ResultSet rs;
        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DbAdminConn.setAutoCommit(false);
            // esegue la query al DB
            String QueryString = "SELECT R.IDBlackBox, R.Record, R.Stato, R.IDRec, R.Time, B.IDAzienda, B.BBserial,R.IDRec,B.IDSWVersion from ZBRecords R"
                    + " LEFT JOIN BlackBox B ON R.IDBlackBox=B.IDBlackBox"
                    //                    + " LEFT JOIN (select IDBlackBox,count(*) C from ZBRecords where Stato=0 group by IDBlackBox) R1 on R1.IDBlackBox=R.IDBlackBox"
                    //                    + " WHERE R.Time< DATE_SUB(NOW(), INTERVAL 10 SECOND)"
                    + " WHERE Stato=0 and R.Time< DATE_SUB(NOW(), INTERVAL 10 SECOND)"
                    //                    + " and R.IDBlackBox=916"
                    //                    + " and R.IDRec>116098896"
                    //                    + " and R.IDRec>=110085088"
                    //                    + " and R.IDRec>=111916475"
                    //                    + "  AND B.IDAzienda<>30"
                    //                    + " ORDER BY R1.C desc ,R.IDBlackBox,R.IDRec"
                    //                    + " ORDER BY R1.C ,R.IDBlackBox Desc,R.IDRec"
                    + " ORDER BY R.IDRec"
                    //                    + " LIMIT 100";
                    //                    + " LIMIT 200";
                    + " LIMIT 20";
            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();
            while (rs.next()) {
                byte Rec[] = rs.getBytes("Record");
                int IDBlackBox = rs.getInt("IDBlackBox");
                //            int IDSWVersion=rs.getInt("IDSWVersion");
                // TODO Verificare il Checksum del record
                byte Type = (byte) (Rec[0] >> 4);
                if (Rec.length < 48) {
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " : dato non corretto");
                    QueryString = "UPDATE ZBRecords SET Stato=3 WHERE IDRec=?";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setLong(1, rs.getLong(4));    //IDRec
                    statement1.execute();
                    statement1.close();
                    continue;
                }
                // Verificare il Checksum del record
//                Log.WriteLog(3,"Verifica CRC per byte="+(Rec.length));
                // verifica del crc
                MessageDigest cript;

                try {
                    cript = MessageDigest.getInstance("SHA-1");
                    cript.reset();
                    cript.update(Rec, 0, 47);
                } catch (NoSuchAlgorithmException ex) {
                    Log.WriteEx(DBAdminClass.class.getName(), ex);
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    continue;
                }

                byte[] CRC = cript.digest();
                if (CRC[0] != Rec[47] && rs.getInt("IDSWVersion") > 17) {
                    Log.WriteLog(4, "Errore sulla verifica del CRC IDRec=" + rs.getLong("IDRec"));
                    QueryString = "UPDATE ZBRecords SET Stato=50 WHERE IDRec=?";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setLong(1, rs.getLong(4));    //IDRec
                    statement1.execute();
                    statement1.close();
                    continue;
                }

                ZBRecord ZBRec = new ZBRecord();
                try {
                    ZBRec.ParseRecord(rs.getLong("IDRec"), IDBlackBox, Rec, rs.getTimestamp("Time"), rs.getInt("IDSWVersion") > 17);
                } catch (ExceptionInvalidRecordLenght ex) {
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    UpdateZBRecordStato(DbAdminConn, rs.getLong("IDRec"), 51);
                    continue;
                } catch (ExceptionCRCError ex) {
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    UpdateZBRecordStato(DbAdminConn, rs.getLong("IDRec"), 50);
                    continue;
                } catch (Exception ex) {
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    continue;
                }

//                if (Type==4) { //record RealTime
                if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordRT) { //record RealTime
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong("IDRec") + " Rec RT");
                    try {
                        InsertZBRecord(DbAdminConn, ZBRec);
                    } catch (SQLException ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DbAdminConn.rollback();
                        return 0;
                    }
                } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordE) {       //  Record Evento
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong("IDRec") + " Rec Evento");
                    try {
                        InsertZBRecord(DbAdminConn, ZBRec);
                    } catch (SQLException ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DbAdminConn.rollback();
                        return 0;
                    }
//                    int TypeEv=uBToI(Rec[1]);
//                    Log.WriteLog(3,"ZB="+rs.getString("BBserial") +" IDRecord ="+rs.getLong(4)+" Rec Ev Type "+TypeEv+" ");
//                    byte B[]=new byte[8];
//                    System.arraycopy(Rec, 2, B, 0, 4);
//                    Timestamp TS=GetTimeStamp(B);
//                    if (TS==null) TS=rs.getTimestamp(5);
//                    if (TS!=null) System.out.print("Timestamp="+TS.toString());
//                    System.arraycopy(Rec, 6, B, 0, 4);
//                    Double Lat= GetCoord(B);
//                    System.arraycopy(Rec, 10, B, 0, 4);
//                    Double Long= GetCoord(B);
//                    byte Extra[]= new byte[10];
//                    System.arraycopy(Rec, 14, Extra, 0, 10);
//                    if (TypeEv==0) {
//                        Log.WriteLog(3," Distress");
//                    } else if(TypeEv==1) {
//                        Log.WriteLog(3," Accelerometrico Lieve");
//                    }else if(TypeEv==2) {
//                        Log.WriteLog(3," Accelerometrico Grave");
//                    }else if(TypeEv==3) {
//                        Log.WriteLog(3," Tamper");
//                    }else if(TypeEv==4) {
//                        Log.WriteLog(3," Sicurezza");
//                    }else if(TypeEv==5) {
//                        Log.WriteLog(3," Aux");
//                    }else if(TypeEv==6) {
//                        Log.WriteLog(3," Assenza Alimentazione Primaria");
//                    }else if(TypeEv==7) {
//                        Log.WriteLog(3," Ripristino Alimentazione Primaria");
//                    }else if(TypeEv==8) {
//                        Log.WriteLog(3," Warning Superamento Velocità");
//                    }else if(TypeEv==9) {
//                        Log.WriteLog(3," Warning Superamento Distanza");
//                    }else if(TypeEv==10) {
//                        Log.WriteLog(3," na");
//                    }else if(TypeEv==11) {
//                        System.out.print(" na");
//                    }else if(TypeEv==12) {
//                        Log.WriteLog(3," Autenticazione Conducente");
//                    }else if(TypeEv==13) {
//                        Log.WriteLog(3," Spostamento a motore spento");
//                    }
//                    //System.out.println("");
//                    Log.WriteLog(3," Lat="+Lat.toString()+" Long="+Long.toString());
//                    try {
//                        QueryString = "INSERT INTO ZBEvents (IDBlackBox,IDType, BLat, BLong, BTimeStamp,Extra,IDRecord) VALUES ("
//                                + " ?, ?, ?, ?,?,?,?)";
//                        statement1 = DbAdminConn.prepareStatement(QueryString);
//                        statement1.setInt(1, IDBlackBox);
//                        statement1.setInt(2, TypeEv);
//                        statement1.setDouble(3, Lat);
//                        statement1.setDouble(4, Long);
//                        statement1.setTimestamp(5, TS);
//                        statement1.setBytes(6, Extra);
//                        statement1.setLong(7, rs.getLong(4));
//                        statement1.execute();
//                        QueryString =  "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
//                        statement1 = DbAdminConn.prepareStatement(QueryString);
//                        statement1.setLong(1,rs.getLong(4));    //IDRec
//                        statement1.execute();
//                        statement1.close();
//                    } catch (SQLException ex) {
//                        Log.WriteEx(DBAdminClass.class.getName(), ex);
//                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//                        DbAdminConn.rollback();
//                        return 0;
//                    }  
//                    
                } else if (Type == 2) {       //  Record Y
                    String LogOut = "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec Y ";
                    Timestamp StartTime = new Timestamp((new java.util.Date()).getTime());
                    int ret = ReadRecY(Rec, rs.getLong(4), IDBlackBox, rs.getTimestamp(5));
                    Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
                    long TimeDiff = now.getTime() - StartTime.getTime();
                    LogOut += " TimeDiff=" + TimeDiff;
                    Log.WriteLog(3, LogOut);
                    if (ret == 0) {
                        DbAdminConn.rollback();
                        return 0;
                    }

                    /*                    try {
                     int IDGuida = ((Rec[0]&0xF)<<16)+(Rec[1]<<8)+Rec[2];
                     Log.WriteLog(3,"IDRecord ="+rs.getInt(4)+" Rec Y ");
                     Timestamp TS=rs.getTimestamp(5);
                        
                     // minuti validi nel record
                     int NumMinutes= Rec[3];
                     if (NumMinutes>8) NumMinutes=8;
                     int FuelLevel= Rec[44];
                        
                     // cerco il record Z di riferimento
                     QueryString = "SELECT RZ.IDZBRecZ, RZ.IDBlackBox, RZ.IDGuida, RZ.IDRecord, RR.Time, "
                     + "RXY.SeqNum, RZ.CertidriveIDGuida from ZBRecz RZ "
                     + "LEFT JOIN ZBRecords RR ON RR.IDRec=RZ.IDRecord "
                     + "LEFT JOIN ZBRecXY RXY ON RXY.IDZBRecZ=RZ.IDZBRecZ "
                     + "WHERE ((RZ.IDGuida=?) and (RR.Time<=?) and (RZ.IDBlackBox=?)) "
                     + "ORDER BY RR.Time desc, RXY.SeqNum desc limit 1";
                    
                     statement = DbAdminConn.prepareStatement(QueryString);
                     statement.setInt(1,IDGuida);
                     statement.setTimestamp(2, TS);
                     statement.setInt(3,IDBlackBox);
                     ResultSet rs1;
                     rs1 = statement.executeQuery();
                     if (rs1.next()) {   // se è stato trovato il record Z di riferimento
                     int IDZBRecZ= rs1.getInt(1);
                     int SeqNum= rs1.getInt(6)+1;
                     int CertidriveIDGuida=rs1.getInt(7);
                     for (int i=0;i<NumMinutes;i++) {
                     byte RecTemp []= new byte[5];
                     System.arraycopy(Rec, 4+(i*5), RecTemp, 0, 5);
                     ReadRecordTemp(RecTemp, IDZBRecZ,SeqNum+ i, FuelLevel,rs.getInt(4));
                     }
                     UpdateCertidriveGuida(IDBlackBox, IDZBRecZ, CertidriveIDGuida);
                     } else {
                     Log.WriteLog(2, "IDRec="+rs.getInt(4)+" Cannot connect record Y to relative Z");
                     }
                        
                     QueryString =  "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setInt(1,rs.getInt(4));    //IDRec
                     statement1.execute();
                     statement1.close();
                     } catch (SQLException ex) {
                     Log.WriteEx(DBAdminClass.class.getName(), ex);
                     Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                     DbAdminConn.rollback();
                     return 0;
                     } catch (Exception ex) {
                     Log.WriteEx(DBAdminClass.class.getName(), ex);
                     Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                     DbAdminConn.rollback();
                     return 0;
                     } */

                } else if (Type == 1) {       //  Record X
                    try {
                        int TypeEv = uBToI(Rec[1]);
                        Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec X Type " + TypeEv + " ");
                        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
                        statement1 = DbAdminConn.prepareStatement(QueryString);
                        statement1.setLong(1, rs.getLong(4));    //IDRec
                        statement1.execute();
                        statement1.close();
                    } catch (SQLException ex) {
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DbAdminConn.rollback();
                        return 0;
                    } catch (Exception ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DbAdminConn.rollback();
                        return 0;
                    }

                } else if (Type == 0) {       //  Record Z
                    Timestamp StartTime = new Timestamp((new java.util.Date()).getTime());
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec Z Type ");
                    int IDGuida = ((uBToI(Rec[0]) & 0xF) << 16) + (uBToI(Rec[1]) << 8) + uBToI(Rec[2]);
                    byte B[] = new byte[8];
                    System.arraycopy(Rec, 4, B, 0, 4);
                    Timestamp TS = ZBRecord.GetTimeStamp(B);
                    if (TS == null) {
                        TS = rs.getTimestamp(5);
                    }
                    if (TS != null) {
                        //System.out.print("Timestamp=" + TS.toString());
                    }
                    System.arraycopy(Rec, 8, B, 0, 4);
                    Double Lat = ZBRecord.GetCoord(B);
                    System.arraycopy(Rec, 12, B, 0, 4);
                    Double Long = ZBRecord.GetCoord(B);
                    int ContaKm = (uBToI(Rec[19]) << 24) + (uBToI(Rec[18]) << 16) + (uBToI(Rec[17]) << 8) + uBToI(Rec[16]);
                    //UCode (TODO)
                    byte TokenSN[] = new byte[4];
                    System.arraycopy(Rec, 20, TokenSN, 0, 4);
                    System.out.println(" Codice Conducente:" + (new String(Hex.encode(TokenSN))).toUpperCase());
                    int IDAzienda = rs.getInt("IDAzienda");
                    Token TK = FindIDToken(TokenSN, IDAzienda);
                    if (TK.IDToken == -1) {
                        TK.IDToken = InsertToken(CDDB, TK);
                    }
                    // minuti validi nel record
                    int NumMinutes = uBToI(Rec[24]) + 1;
                    if (NumMinutes > 4) {
                        NumMinutes = 4;
                    }
                    // durata intervallo
                    int DurataIntervallo = uBToI(Rec[25]);
                    // FuelLevel
                    int FuelLevel = uBToI(Rec[46]);

                    try {
                        QueryString = "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=?";
                        statement1 = DbAdminConn.prepareStatement(QueryString);
                        statement1.setInt(1, IDBlackBox);  //IDBlackBox
                        statement1.execute();
                        statement1.close();
                        QueryString = "INSERT INTO ZBRecZ (IDBlackBox, IDGuida, ContaKm, FuelLevel, BLat, BLong, BTimeStamp,"
                                + "IDToken, IDRecord, DurataIntervallo) VALUES ("
                                + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        statement1 = DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                        statement1.setInt(1, IDBlackBox);  //IDBlackBox
                        statement1.setInt(2, IDGuida);
                        statement1.setInt(3, ContaKm);
                        statement1.setInt(4, FuelLevel);
                        statement1.setDouble(5, Lat);
                        statement1.setDouble(6, Long);
                        statement1.setTimestamp(7, TS);
                        statement1.setLong(8, TK.IDToken);
                        statement1.setLong(9, rs.getLong(4));    //IDRec
                        statement1.setInt(10, DurataIntervallo);
                        statement1.execute();
                        ResultSet generatedKeys;
                        generatedKeys = statement1.getGeneratedKeys();
                        long IDRecZ;
                        if (generatedKeys.next()) {
                            IDRecZ = generatedKeys.getLong(1);
                            for (int i = 0; i < NumMinutes; i++) {
                                byte RecTemp[] = new byte[5];
                                System.arraycopy(Rec, 26 + (i * 5), RecTemp, 0, 5);
                                ReadRecordTemp(RecTemp, IDRecZ, i, FuelLevel, rs.getLong(4));
                            }
                        }
                        if (NumMinutes < 4) {
                            QueryString = "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=?";
                            statement1 = DbAdminConn.prepareStatement(QueryString);
                            statement1.setInt(1, IDBlackBox);  //IDBlackBox
                            statement1.execute();
                            statement1.close();
                        }
                        // inserisco i dati della guida nel DB di Certidrive
/*                        int CertidriveIDGuida=InsertCertidriveGuida (rs.getInt(1),TK.CDIDconducente,IDGuida,TS,FuelLevel);
                        
                         // aggiorno lo stato del record
                         if (CertidriveIDGuida!= -1) {
                         QueryString =  "UPDATE ZBRecZ SET CertidriveIDGuida=? WHERE IDZBRecZ=?";
                         statement1 = DbAdminConn.prepareStatement(QueryString);
                         statement1.setInt(1,CertidriveIDGuida);    //CertidriveIDGuida
                         statement1.setInt(2,IDRecZ);    //IDRec
                         statement1.execute();
                         statement1.close();
                         } else {
                         Log.WriteLog(3,"Errore di registrazione del record ID "+rs.getInt(4)+" in CertidriveWeb");
                         }*/
                        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
                        statement1 = DbAdminConn.prepareStatement(QueryString);
                        statement1.setLong(1, rs.getLong(4));    //IDRec
                        statement1.execute();
                        statement1.close();
                    } catch (SQLException ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        QueryString = "UPDATE ZBRecords SET Stato=4 WHERE IDRec=?";
                        statement1 = DbAdminConn.prepareStatement(QueryString);
                        statement1.setLong(1, rs.getLong(4));    //IDRec
                        statement1.execute();
                        statement1.close();
//                        DbAdminConn.rollback();
                        return 0;
                    } catch (Exception ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        QueryString = "UPDATE ZBRecords SET Stato=4 WHERE IDRec=?";
                        statement1 = DbAdminConn.prepareStatement(QueryString);
                        statement1.setLong(1, rs.getLong(4));    //IDRec
                        statement1.execute();
                        statement1.close();
//                        DbAdminConn.rollback();
                        return 0;
                    }
                    Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
                    long TimeDiff = now.getTime() - StartTime.getTime();
//                    System.out.println("TimeDiff="+TimeDiff);
                    //System.out.println("ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec Z Type TimeDiff=" + TimeDiff);

                } else if (Type == 5) {       //  Record T
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec T ");
                    long IDRec = rs.getLong(4);
                    int ret;
                    ret = ReadRecT(Rec, IDRec, IDBlackBox, rs.getTimestamp(5));
                    if (ret == 0) {
                        DbAdminConn.rollback();
                        return 0;
                    }
                } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordI) {       //  Record I
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec I ");
//                    long IDRec=rs.getLong(4);
//                    int ret;
//                    ret=ReadRecI(Rec, IDRec, IDBlackBox, rs.getTimestamp(5));
//                    if (ret == 0) {
//                        DbAdminConn.rollback();
//                        return 0;
//                    }
                    try {
                        InsertZBRecord(DbAdminConn, ZBRec);
                    } catch (SQLException ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DbAdminConn.rollback();
                        return 0;
                    }
                } else {
                    try {
                        QueryString = "UPDATE ZBRecords SET Stato=3 WHERE IDRec=?";
                        statement1 = DbAdminConn.prepareStatement(QueryString);
                        statement1.setLong(1, rs.getLong(4));    //IDRec
                        statement1.execute();
                        statement1.close();
                    } catch (SQLException ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DbAdminConn.rollback();
                        return 0;
                    }
                }
            }
//            System.out.println("Fine");
            rs.close();
            statement.close();
            if (CDDB != null) {
                if (!CDDB.isClosed()) {
                    CDDB.commit();
                    CDDB.close();
                }
            }
            DbAdminConn.commit();
            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Legge ed archivia i record T nel DB
     *
     * @param Rec: record da archiviare
     * @param IDRecord: identificativo del record nel DB
     * @param IDBlackBox: identificativo della ZB
     * @param TS: Timestamp di ricezione del record
     * @return
     */
    public int ReadRecT(byte Rec[], long IDRecord, int IDBlackBox, Timestamp TS) {
        PreparedStatement statement;
        try {
            String QueryString;

            // primo blocco di telemetrie
            int Payload1Size = (uBToI(Rec[3]) & 0x0F);
            int TipoTelemetria1 = ((uBToI(Rec[3]) & 0xF0) >> 4);
            long PID1 = ((uBToI(Rec[11]) & 0xFF) << 24)
                    + ((uBToI(Rec[10]) & 0xFF) << 16)
                    + ((uBToI(Rec[9]) & 0xFF) << 8)
                    + +(uBToI(Rec[8]) & 0xFF);
            byte Payload1[] = new byte[12];
            if (Payload1Size > 12
                    || (TipoTelemetria1 != 0 && TipoTelemetria1 != 1 && TipoTelemetria1 != 2 && TipoTelemetria1 != 3
                    && TipoTelemetria1 != 4 && TipoTelemetria1 != 5)) {
                Log.WriteLog(1, "Errore nel record Telemetria IDRec=" + IDRecord);
                QueryString = "UPDATE ZBRecords SET Stato=3 WHERE IDRec=?";
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setLong(1, IDRecord);    //IDRec
                statement.execute();
                statement.close();
                return 1;
            }
            System.arraycopy(Rec, 12, Payload1, 0, Payload1Size);
            if (TipoTelemetria1 == 3) {        // sensore termico
                int Temp[] = new int[6];
                int i;
                for (i = 0; i < 6; i++) {
                    if ((uBToI(Payload1[2 * i + 1]) & 0x80) == 0x80) {
                        Temp[i] = -(((Utils.uBToI(Payload1[2 * i + 1]) & 0x7F) << 8) + Utils.uBToI(Payload1[2 * i]));
                    } else {
                        Temp[i] = ((Utils.uBToI(Payload1[2 * i + 1]) & 0x7F) << 8) + Utils.uBToI(Payload1[2 * i]);
                    }
                }
                Log.WriteLog(3, "Temp[0]=" + Temp[0] + " Temp[1]=" + Temp[1] + " Temp[2]=" + Temp[2] + " Temp[3]=" + Temp[3] + " Temp[4]=" + Temp[4] + " Temp[5]=" + Temp[5]);
            } else if (TipoTelemetria1 == 4) {        // TPMS
                int Payload2Size = (uBToI(Rec[44]) & 0x0F);

                Payload1 = new byte[24];
                System.arraycopy(Rec, 12, Payload1, 0, Payload1Size);
                System.arraycopy(Rec, 32, Payload1, 12, Payload2Size);
            } else if (TipoTelemetria1 == 5) {        // HHO
                int Payload2Size = (uBToI(Rec[44]) & 0x0F);

                Payload1 = new byte[24];
                System.arraycopy(Rec, 12, Payload1, 0, Payload1Size);
                System.arraycopy(Rec, 32, Payload1, 12, Payload2Size);
                Log.WriteLog(3, "Telemetry Payload=" + Utils.toHexString(Payload1));
            }
            byte B[] = new byte[8];
            System.arraycopy(Rec, 4, B, 0, 4);
            Timestamp BTime = ZBRecord.GetTimeStamp(B);
            if (BTime == null) {
                BTime = TS;
            }
            if (BTime != null) {
                //System.out.print("Timestamp=" + BTime.toString());
            }

            QueryString = "INSERT INTO ZBTelemetry (IDBlackBox, Type, PID, Payload, Stato, IDRecord , BTimeStamp) VALUES ("
                    + " ?, ?, ?,?, 0, ?, ?)";

            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            statement.setInt(2, TipoTelemetria1);
            statement.setLong(3, PID1);
            statement.setBytes(4, Payload1);
            statement.setLong(5, IDRecord);
            statement.setTimestamp(6, BTime);

            statement.execute();
            statement.close();

            // secondo blocco

            QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setLong(1, IDRecord);    //IDRec
            statement.execute();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            DbAdminConn.rollback();
            return 0;
        } catch (Exception ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            DbAdminConn.rollback();
            String QueryString = "UPDATE ZBRecords SET Stato=3 WHERE IDRec=?";
            try {
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setLong(1, IDRecord);    //IDRec
                statement.execute();
                statement.close();
            } catch (SQLException Ex) {
                return 0;
            }
            return 1;
        }
        return 1;
    }

    /**
     * Legge ed archivia i record I nel DB
     *
     * @param Rec: record da archiviare
     * @param IDRecord: identificativo del record nel DB
     * @param IDBlackBox: identificativo della ZB
     * @param TS: Timestamp di ricezione del record
     * @return
     */
    public int ReadRecI(byte Rec[], long IDRecord, int IDBlackBox, Timestamp TS) {
        PreparedStatement statement;
        try {
            String QueryString;
            byte B[] = new byte[8];
            int i;
            String DateVersion = "";
            String TimeVersion = "";
            int ErrNumber = (uBToI(Rec[1]) & 0xFF);
            int MaiorVersion = (uBToI(Rec[2]) & 0xFF);
            int MinorVersion = (uBToI(Rec[3]) & 0xFF);
            long LastOperativeTime = ((uBToI(Rec[7]) & 0xFF) << 24)
                    + ((uBToI(Rec[6]) & 0xFF) << 16)
                    + ((uBToI(Rec[5]) & 0xFF) << 8)
                    + +(uBToI(Rec[4]) & 0xFF);

            int HWRev = (uBToI(Rec[31]) & 0xFF);

            if ((MaiorVersion > 99) || (MinorVersion > 99)) {
                Log.WriteLog(1, "Errore nel record informativo IDRec=" + IDRecord);
                QueryString = "UPDATE ZBRecords SET Stato=3 WHERE IDRec=?";
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setLong(1, IDRecord);    //IDRec
                statement.execute();
                statement.close();
                return 1;
            }

            for (i = 0; i < 11; i++) {
                DateVersion += (char) Rec[12 + i];
            }
            for (i = 0; i < 8; i++) {
                TimeVersion += (char) Rec[23 + i];
            }

            System.arraycopy(Rec, 8, B, 0, 4);
            Timestamp BTime = ZBRecord.GetTimeStamp(B);
            if (BTime == null) {
                BTime = TS;
            }
            if (BTime != null) {
                //System.out.print("Timestamp=" + BTime.toString());
            }

            QueryString = "INSERT INTO ZBInfo(IDBlackBox, ErrorsNumber, MaiorVersion, MinorVersion,"
                    + " DateVersion, TimeVersion, LastOperativeTime, IDRecord , BTimeStamp, HWRev) VALUES ("
                    + " ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";

            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            statement.setInt(2, ErrNumber);
            statement.setInt(3, MaiorVersion);
            statement.setInt(4, MinorVersion);
            statement.setString(5, DateVersion);
            statement.setString(6, TimeVersion);
            statement.setLong(7, LastOperativeTime);
            statement.setLong(8, IDRecord);
            statement.setTimestamp(9, BTime);
            statement.setInt(10, HWRev);
            statement.execute();
            statement.close();

            QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setLong(1, IDRecord);    //IDRec
            statement.execute();
            statement.close();


            // verifica della versione 
            QueryString = "SELECT IDSWVersion, SWVersion, DataRilascio,MaiorVersion, MinorVersion,IDBlackBoxType FROM swversion "
                    + "WHERE IDBlackBoxType=2 and MaiorVersion=? and MinorVersion=? "
                    + "ORDER BY IDSWVersion desc limit 1";

            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, MaiorVersion);
            statement.setInt(2, MinorVersion);
            ResultSet rs1;
            rs1 = statement.executeQuery();
            if (rs1.next()) {
                Log.WriteLog(1, "SW Version " + MaiorVersion + "." + MinorVersion + " ID=" + rs1.getInt("IDSWVersion"));
                QueryString = "UPDATE BlackBox SET IDSWVersion=? WHERE IdBlackBox=?";
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setInt(1, rs1.getInt("IDSWVersion"));    //IDSWVersion
                statement.setInt(2, IDBlackBox);    //IDBlackBox
                statement.execute();
                statement.close();
            }
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        } catch (Exception ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
        return 1;
    }

    public int ReadRecY(byte Rec[], long IDRecord, int IDBlackBox, Timestamp TS) {
        PreparedStatement statement, statement1;
        try {
            int IDGuida = ((uBToI(Rec[0]) & 0xF) << 16) + (uBToI(Rec[1]) << 8) + uBToI(Rec[2]);

            // minuti validi nel record
            int NumMinutes = uBToI(Rec[3]);
            if (NumMinutes > 8) {
                NumMinutes = 8;
            }
            int FuelLevel = uBToI(Rec[44]);

            // cerco il record Z di riferimento
            String QueryString = "SELECT RZ.IDZBRecZ, RZ.IDBlackBox, RZ.IDGuida, RZ.IDRecord, RR.Time, "
                    + "RXY.SeqNum, RZ.CertidriveIDGuida from ZBRecz RZ "
                    + "LEFT JOIN ZBRecords RR ON RR.IDRec=RZ.IDRecord "
                    + "LEFT JOIN ZBRecXY RXY ON RXY.IDZBRecZ=RZ.IDZBRecZ "
                    + "WHERE ((RZ.IDGuida=?) and (RR.Time<=?) and (RZ.IDBlackBox=?)) "
                    + "ORDER BY RR.Time desc, RXY.SeqNum desc limit 1";

            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, IDGuida);
            statement.setTimestamp(2, TS);
            statement.setInt(3, IDBlackBox);
            ResultSet rs1;
            rs1 = statement.executeQuery();
            if (rs1.next()) {   // se è stato trovato il record Z di riferimento
                int IDZBRecZ = rs1.getInt(1);
                int SeqNum = rs1.getInt(6) + 1;
                int CertidriveIDGuida = rs1.getInt(7);
                for (int i = 0; i < NumMinutes; i++) {
                    byte RecTemp[] = new byte[5];
                    System.arraycopy(Rec, 4 + (i * 5), RecTemp, 0, 5);
                    ReadRecordTemp(RecTemp, IDZBRecZ, SeqNum + i, FuelLevel, IDRecord);
                }
//                UpdateCertidriveGuida(IDBlackBox, IDZBRecZ, CertidriveIDGuida);
                QueryString = "UPDATE ZBRecZ SET Stato=0 WHERE IDZBRecZ=?";
                statement1 = DbAdminConn.prepareStatement(QueryString);
                statement1.setLong(1, IDZBRecZ);    //IDRec
                statement1.execute();
                statement1.close();
                if (NumMinutes < 8) {
                    QueryString = "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=?";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setInt(1, IDBlackBox);  //IDBlackBox
                    statement1.execute();
                    statement1.close();
                }
            } else {
                Log.WriteLog(2, "IDRec=" + IDRecord + " Cannot connect record Y to relative Z");
            }
            rs1.close();
            statement.close();

            QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
            statement1 = DbAdminConn.prepareStatement(QueryString);
            statement1.setLong(1, IDRecord);    //IDRec
            statement1.execute();
            statement1.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            DbAdminConn.rollback();
            return 0;
        } catch (Exception ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            DbAdminConn.rollback();
            return 0;
        }
        return 1;
    }

    public int ReadRecX(byte Rec[], long IDRecord, int IDBlackBox, Timestamp TS) {
        /*        PreparedStatement statement1;
         try {
         int TypeEv=uBToI(Rec[1]);
         Log.WriteLog(3,"IDRecord ="+rs.getLong(4)+" Rec X Type "+TypeEv+" ");
         String QueryString =  "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
         statement1 = DbAdminConn.prepareStatement(QueryString);
         statement1.setLong(1,rs.getLong(4));    //IDRec
         statement1.execute();
         statement1.close();
         } catch (SQLException ex) {
         Log.WriteEx(DBAdminClass.class.getName(), ex);
         Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
         //                        DbAdminConn.rollback();
         return 0;
         } catch (Exception ex) {
         Log.WriteEx(DBAdminClass.class.getName(), ex);
         Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
         DbAdminConn.rollback();
         return 0;
         }*/
        return 1;
    }

    /**
     * inserisce un record all'interno dell corretto DB CertidriveWeb
     *
     * @param IDBlackBox
     * @param IDConducente
     * @param NrGuida
     * @param TS
     * @param FuelLevel
     * @return -1 se inserimento fallito altrimenti ritorno lo ID del record
     * inserito.
     */
    private int InsertCertidriveGuida(int IDBlackBox, long IDZBRecZ, int IDConducente, int NrGuida, Timestamp TS, int FuelLevel) {
        PreparedStatement statement, statement1;
        ResultSet rs;
        ResultSet rs1;
        long CCode;
        long VCode;
        int MinutiGuida = 1;
        int DistanzaGuida = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            String QueryString = "SELECT IDBlackBox, B.IDAzienda, CertidriveIDVeicolo, CertidriveIDAzienda from BlackBox B "
                    + "LEFT JOIN Azienda A ON A.IDAzienda=B.IDAzienda WHERE B.IDBlackBox=?";
            statement1 = DbAdminConn.prepareStatement(QueryString);
            statement1.setInt(1, IDBlackBox);
            rs1 = statement1.executeQuery();
            if (rs1.next()) {
                try {
                    CDDB = CDDdAdminClass.OpenAZDBConn(rs1.getInt("CertidriveIDAzienda"));
                    QueryString = "SELECT CCODE FROM conducenti WHERE ID=" + IDConducente;
                    statement = CDDB.prepareStatement(QueryString);
                    rs = statement.executeQuery(QueryString);
                    if (!rs.next()) {
                        CCode = 0;
                    } else {
                        CCode = rs.getLong(1);
                    }


                    QueryString = "SELECT VCODE FROM veicoli WHERE ID=" + rs1.getInt("CertidriveIDVeicolo");
                    statement = CDDB.prepareStatement(QueryString);
                    rs = statement.executeQuery(QueryString);

                    if (!rs.next()) {
                        VCode = -1;
                    } else {
                        VCode = rs.getLong(1);
                    }

                    QueryString = "INSERT INTO driverecords (NRGUIDA, OPMODE, KEYALARM, CCODE, VCODE, DATAORAINIZIO,"
                            + "MINUTI, DISTANZAPERCORSA, DISTANZAVELOCITAA, DISTANZAVELOCITAB, DISTANZAVELOCITAC,"
                            + " DISTANZAVELOCITAD, LATINIZ, LONGINIZ, LIVCARBINIZ, LIVCARBFIN, ORIGINE, AUTENTICATED,"
                            + " MATRICOLA) values (" + NrGuida + ", 0, 0, " + CCode + ", "
                            + VCode + ", '" + sdf.format(TS) + "', " + MinutiGuida + ", "
                            + DistanzaGuida + ", 0,0,0,0, 0, 0, " + FuelLevel + ", " + FuelLevel + ", 'V', 0,0)";

                    statement1 = CDDB.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                    Log.WriteLog(2, "InsertCertidriveGuida: IDBlackBox=" + IDBlackBox + " IDRecZ=" + IDZBRecZ
                            + "  NRGUIDA=" + NrGuida + " Min=" + MinutiGuida + " CCODE=" + CCode + " VCODE=" + VCode
                            + " DATAORAINIZIO='" + sdf.format(TS) + "'"
                            + " Fuel=" + FuelLevel);
                    statement1.execute();
                    ResultSet generatedKeys;
                    generatedKeys = statement1.getGeneratedKeys();
                    if (!generatedKeys.next()) {
                        rs1.close();
                        CDDB.close();
                        return -1;
                    }
                    int ret = generatedKeys.getInt(1);
                    rs1.close();
                    CDDB.close();

                    UpdateCertidriveGuida(IDBlackBox, IDZBRecZ, IDConducente, NrGuida, ret, TS, FuelLevel);

                    return ret;
                } catch (Exception ex) {
                    Log.WriteEx(DBAdminClass.class.getName(), ex);
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            rs1.close();
            CDDB.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }
        return -1;
    }

    public int UpdateCertidriveGuida(int IDBlackBox, long IDZBRecZ, int IDConducente,
            int NrGuida, int CertidriveIDGuida, Timestamp TS, int FuelLevel) throws Exception {
        PreparedStatement statement, statement1;

        ResultSet rs, rs1;
        long CCode;
        long VCode;

        try {
            String QueryString = "SELECT IDBlackBox, B.IDAzienda, CertidriveIDVeicolo, CertidriveIDAzienda from BlackBox B "
                    + "LEFT JOIN Azienda A ON A.IDAzienda=B.IDAzienda WHERE B.IDBlackBox=?";
            statement1 = DbAdminConn.prepareStatement(QueryString);
            statement1.setInt(1, IDBlackBox);
            rs1 = statement1.executeQuery();
            if (rs1.next()) {
                try {
                    CDDB = CDDdAdminClass.OpenAZDBConn(rs1.getInt("CertidriveIDAzienda"));
                    QueryString = "SELECT CCODE FROM conducenti WHERE ID=" + IDConducente;
                    statement = CDDB.prepareStatement(QueryString);
                    rs = statement.executeQuery();
                    if (!rs.next()) {
                        CCode = 0;
                    } else {
                        CCode = rs.getLong(1);
                    }


                    QueryString = "SELECT VCODE FROM veicoli WHERE ID=" + rs1.getInt("CertidriveIDVeicolo");
                    statement = CDDB.prepareStatement(QueryString);
                    rs = statement.executeQuery();

                    if (!rs.next()) {
                        VCode = -1;
                        return 0;
                    } else {
                        VCode = rs.getLong(1);
                    }

                    rs1.close();
                    statement1.close();

                    QueryString = "SELECT IDZBRecXY, SeqNum, Metri, DeltaLat, DeltaLong, Eventi, FuelLevel from ZBRecXY "
                            + " WHERE IDZBRecZ=? ORDER BY SeqNum";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setLong(1, IDZBRecZ);
                    rs1 = statement1.executeQuery();

                    int minguida = 0;
                    int distguida = 0;
                    int ActDist;
                    int v1 = 0, v2 = 0, v3 = 0, v4 = 0;
                    while (rs1.next()) {
                        ActDist = rs1.getInt(3);
                        FuelLevel = rs1.getInt(7);
                        distguida += ActDist;
                        minguida++;
                        if (ActDist > (Conf.Vlim1 * 100 / 6) && ActDist <= (Conf.Vlim2 * 100 / 6)) {
                            v1 += ActDist;
                        }
                        if (ActDist > (Conf.Vlim2 * 100 / 6) && ActDist <= (Conf.Vlim3 * 100 / 6)) {
                            v2 += ActDist;
                        }
                        if (ActDist > (Conf.Vlim3 * 100 / 6) && ActDist <= (Conf.Vlim4 * 100 / 6)) {
                            v3 += ActDist;
                        }
                        if (ActDist > (Conf.Vlim4 * 100 / 6)) {
                            v4 += ActDist;
                        }
                    }

                    QueryString = "UPDATE driverecords SET Minuti=?,DISTANZAPERCORSA=?, DISTANZAVELOCITAA=?, "
                            + "DISTANZAVELOCITAB=?, DISTANZAVELOCITAC=?, DISTANZAVELOCITAD=?, LATINIZ=?, "
                            + "LONGINIZ=?, LIVCARBFIN=?, CCODE=" + CCode + " , VCODE=" + VCode + "  WHERE ID=?";
                    /*                    QueryString = "INSERT INTO driverecords (NRGUIDA, OPMODE, KEYALARM, CCODE, VCODE, DATAORAINIZIO,"+
                     "MINUTI, DISTANZAPERCORSA, DISTANZAVELOCITAA, DISTANZAVELOCITAB, DISTANZAVELOCITAC,"+
                     " DISTANZAVELOCITAD, LATINIZ, LONGINIZ, LIVCARBINIZ, LIVCARBFIN, ORIGINE, AUTENTICATED,"+
                     " MATRICOLA) values ("+NrGuida +", 0, 0, "+CCode + ", "+
                     VCode + ", '"+sdf.format(TS)+ "', "+ MinutiGuida+", "+
                     DistanzaGuida +", 0,0,0,0, 0, 0, "+FuelLevel+ ", "+FuelLevel+", 'V', 0,0)";*/

                    statement1 = CDDB.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                    statement1.setInt(1, minguida);
                    statement1.setDouble(2, ((double) distguida) / 1000);

                    statement1.setDouble(3, ((double) v1) / 1000);
                    statement1.setDouble(4, ((double) v2) / 1000);
                    statement1.setDouble(5, ((double) v3) / 1000);
                    statement1.setDouble(6, ((double) v4) / 1000);
                    statement1.setInt(7, 0);     // latIniz
                    statement1.setInt(8, 0);     // LongIniz
                    statement1.setInt(9, FuelLevel);   // FuelLevel
                    statement1.setInt(10, CertidriveIDGuida);

                    Log.WriteLog(2, "UpdateCertidriveGuida: IDBlackBox=" + IDBlackBox + " IDRecZ=" + IDZBRecZ
                            + " CertidriveIDGuida=" + CertidriveIDGuida + " DIST=" + distguida / 100 + " Min=" + minguida
                            + " DISTv1=" + v1 / 100 + " DISTv2=" + v2 / 100 + " DISTv3=" + v3 / 100 + " DISTv4=" + v4 / 100
                            + " Fuel=" + FuelLevel + " CCODE=" + CCode + " , VCODE=" + VCode);
                    statement1.execute();

                    rs1.close();

                    CDDB.close();
                    return 1;
                } catch (Exception ex) {
                    Log.WriteEx(DBAdminClass.class.getName(), ex);
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            throw new Exception("UpdateCertidriveGuida Errore SQL ");
        }
        throw new Exception("UpdateCertidriveGuida Aggiornamento NON avvenuto correttamente ");
    }

    public int UpdateCertidriveGuida_old(int IDBlackBox, long IDZBRecZ, int IDConducente,
            int NrGuida, int CertidriveIDGuida, Timestamp TS, int FuelLevel) throws Exception {
        PreparedStatement statement, statement1;

        ResultSet rs, rs1;
        long CCode;
        long VCode;

        try {
            String QueryString = "SELECT IDBlackBox, B.IDAzienda, CertidriveIDVeicolo, CertidriveIDAzienda from BlackBox B "
                    + "LEFT JOIN Azienda A ON A.IDAzienda=B.IDAzienda WHERE B.IDBlackBox=?";
            statement1 = DbAdminConn.prepareStatement(QueryString);
            statement1.setInt(1, IDBlackBox);
            rs1 = statement1.executeQuery();
            if (rs1.next()) {
                try {
                    CDDB = CDDdAdminClass.OpenAZDBConn(rs1.getInt("CertidriveIDAzienda"));
                    QueryString = "SELECT CCODE FROM conducenti WHERE ID=" + IDConducente;
                    statement = CDDB.prepareStatement(QueryString);
                    rs = statement.executeQuery();
                    if (!rs.next()) {
                        CCode = 0;
                    } else {
                        CCode = rs.getLong(1);
                    }


                    QueryString = "SELECT VCODE FROM veicoli WHERE ID=" + rs1.getInt("CertidriveIDVeicolo");
                    statement = CDDB.prepareStatement(QueryString);
                    rs = statement.executeQuery();

                    if (!rs.next()) {
                        VCode = -1;
                        return 0;
                    } else {
                        VCode = rs.getLong(1);
                    }

                    rs1.close();
                    statement1.close();

                    QueryString = "SELECT IDZBRecXY, SeqNum, Metri, DeltaLat, DeltaLong, Eventi, FuelLevel from ZBRecXY_old "
                            + " WHERE IDZBRecZ=?"
                            + " ORDER BY SeqNum";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setLong(1, IDZBRecZ);
                    rs1 = statement1.executeQuery();

                    int minguida = 0;
                    int distguida = 0;
                    int ActDist;
                    int v1 = 0, v2 = 0, v3 = 0, v4 = 0;
                    while (rs1.next()) {
                        ActDist = rs1.getInt(3);
                        FuelLevel = rs1.getInt(7);
                        distguida += ActDist;
                        minguida++;
                        if (ActDist > (Conf.Vlim1 * 100 / 6) && ActDist <= (Conf.Vlim2 * 100 / 6)) {
                            v1 += ActDist;
                        }
                        if (ActDist > (Conf.Vlim2 * 100 / 6) && ActDist <= (Conf.Vlim3 * 100 / 6)) {
                            v2 += ActDist;
                        }
                        if (ActDist > (Conf.Vlim3 * 100 / 6) && ActDist <= (Conf.Vlim4 * 100 / 6)) {
                            v3 += ActDist;
                        }
                        if (ActDist > (Conf.Vlim4 * 100 / 6)) {
                            v4 += ActDist;
                        }
                    }

                    QueryString = "UPDATE driverecords SET Minuti=?,DISTANZAPERCORSA=?, DISTANZAVELOCITAA=?, "
                            + "DISTANZAVELOCITAB=?, DISTANZAVELOCITAC=?, DISTANZAVELOCITAD=?, LATINIZ=?, "
                            + "LONGINIZ=?, LIVCARBFIN=?, CCODE=" + CCode + " , VCODE=" + VCode + "  WHERE ID=?";
                    /*                    QueryString = "INSERT INTO driverecords (NRGUIDA, OPMODE, KEYALARM, CCODE, VCODE, DATAORAINIZIO,"+
                     "MINUTI, DISTANZAPERCORSA, DISTANZAVELOCITAA, DISTANZAVELOCITAB, DISTANZAVELOCITAC,"+
                     " DISTANZAVELOCITAD, LATINIZ, LONGINIZ, LIVCARBINIZ, LIVCARBFIN, ORIGINE, AUTENTICATED,"+
                     " MATRICOLA) values ("+NrGuida +", 0, 0, "+CCode + ", "+
                     VCode + ", '"+sdf.format(TS)+ "', "+ MinutiGuida+", "+
                     DistanzaGuida +", 0,0,0,0, 0, 0, "+FuelLevel+ ", "+FuelLevel+", 'V', 0,0)";*/

                    statement1 = CDDB.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                    statement1.setInt(1, minguida);
                    statement1.setDouble(2, ((double) distguida) / 1000);

                    statement1.setDouble(3, ((double) v1) / 1000);
                    statement1.setDouble(4, ((double) v2) / 1000);
                    statement1.setDouble(5, ((double) v3) / 1000);
                    statement1.setDouble(6, ((double) v4) / 1000);
                    statement1.setInt(7, 0);     // latIniz
                    statement1.setInt(8, 0);     // LongIniz
                    statement1.setInt(9, FuelLevel);   // FuelLevel
                    statement1.setInt(10, CertidriveIDGuida);

                    Log.WriteLog(2, "UpdateCertidriveGuida: IDBlackBox=" + IDBlackBox + " IDRecZ=" + IDZBRecZ
                            + " CertidriveIDGuida=" + CertidriveIDGuida + " DIST=" + distguida / 100 + " Min=" + minguida
                            + " DISTv1=" + v1 / 100 + " DISTv2=" + v2 / 100 + " DISTv3=" + v3 / 100 + " DISTv4=" + v4 / 100
                            + " Fuel=" + FuelLevel);
                    statement1.execute();

                    rs1.close();

                    CDDB.close();
                    return 1;
                } catch (Exception ex) {
                    Log.WriteEx(DBAdminClass.class.getName(), ex);
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            throw new Exception("UpdateCertidriveGuida Errore SQL ");
        }
        throw new Exception("UpdateCertidriveGuida Aggiornamento NON avvenuto correttamente ");
    }

    /**
     * Inserisce un record nel DB se non presente
     *
     * @param ZB
     * @param rec
     * @return
     */
    public int ReadRecordTemp(byte RecTemp[], long IDRecZ, int SeqNum, int FuelLevel, Long IDRecord) {
        PreparedStatement statement;

        try {
            int DistanzaPercorsa = ((uBToI(RecTemp[0])) << 4) + ((uBToI(RecTemp[1]) >> 4) & 0xF);
            int DeltaLat = ((uBToI(RecTemp[1]) & 0x7) << 8) + uBToI(RecTemp[2]);
            if (((uBToI(RecTemp[1]) & 0x8) >> 3) > 0) {
                DeltaLat = -(DeltaLat);
            }

            int DeltaLong = (((uBToI(RecTemp[3])) & 0x7F) << 4) + ((uBToI(RecTemp[4]) >> 4) & 0xF);
            if (((uBToI(RecTemp[3]) & 0x80) >> 7) > 0) {
                DeltaLong = -(DeltaLong);
            }

            int Eventi = (uBToI(RecTemp[4]) & 0xF);
            try {
                String QueryString = "INSERT INTO ZBRecXY (IDZBRecZ, SeqNum, Metri, DeltaLat, DeltaLong, Eventi, FuelLevel,IDRecord) VALUES ("
                        + " ?, ?, ?, ?, ?, ?, ?,?)";
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setLong(1, IDRecZ);  //IDRecZ
                statement.setInt(2, SeqNum);
                statement.setInt(3, DistanzaPercorsa);
                statement.setInt(4, DeltaLat);
                statement.setInt(5, DeltaLong);
                statement.setInt(6, Eventi);
                statement.setInt(7, FuelLevel);
                statement.setLong(8, IDRecord);
                statement.execute();
            } catch (SQLException ex) {
                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                return 0;
            }
            statement.close();
            return 1;
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Inserisce un record nel DB se non presente
     *
     * @param ZB
     * @param rec
     * @return
     */
    public int UpdateRecordTemp(byte RecTemp[], long IDRecZ, int SeqNum, int FuelLevel, Long IDRecord) {
        PreparedStatement statement;
        try {
            int DistanzaPercorsa = ((uBToI(RecTemp[0])) << 4) + ((uBToI(RecTemp[1]) >> 4) & 0xF);
            int DeltaLat = (uBToI(RecTemp[1]) & 0xF) + uBToI(RecTemp[2]);
            int DeltaLong = ((uBToI(RecTemp[3])) << 4) + ((uBToI(RecTemp[4]) >> 4) & 0xF);
            int Eventi = (uBToI(RecTemp[4]) & 0xF);
            try {
                String QueryString = "UPDATE ZBRecXY SET IDZBRecZ=?, SeqNum=?, Metri=?, DeltaLat=?, DeltaLong=?, Eventi=?, FuelLevel=?,IDRecord=?"
                        + " WHERE SeqNum=? AND IDRecord=?";
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setLong(1, IDRecZ);  //IDRecZ
                statement.setInt(2, SeqNum);
                statement.setInt(3, DistanzaPercorsa);
                statement.setInt(4, DeltaLat);
                statement.setInt(5, DeltaLong);
                statement.setInt(6, Eventi);
                statement.setInt(7, FuelLevel);
                statement.setLong(8, IDRecord);
                statement.setInt(9, SeqNum);
                statement.setLong(10, IDRecord);  //IDRecord
                int ret = statement.executeUpdate();
                if (ret == 0) {
                    QueryString = "INSERT INTO ZBRecXY (IDZBRecZ, SeqNum, Metri, DeltaLat, DeltaLong, Eventi, FuelLevel,IDRecord) VALUES ("
                            + " ?, ?, ?, ?, ?, ?, ?,?)";
                    statement = DbAdminConn.prepareStatement(QueryString);
                    statement.setLong(1, IDRecZ);  //IDRecZ
                    statement.setInt(2, SeqNum);
                    statement.setInt(3, DistanzaPercorsa);
                    statement.setInt(4, DeltaLat);
                    statement.setInt(5, DeltaLong);
                    statement.setInt(6, Eventi);
                    statement.setInt(7, FuelLevel);
                    statement.setLong(8, IDRecord);
                    statement.execute();
                }
            } catch (SQLException ex) {
                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                return 0;
            }
            statement.close();
            return 1;
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Inserisce un record R ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    public int InsertZBRecord(Connection Conn, ZBRecord R) throws SQLException {
        if (R.getRecordType() == ZBRecord.RecordTypes.RecordRT) {
            if (Conf.DisableDBWriteData == 0) {
                InsertZBRecordRT(Conn, R);
            } else {
                // TODO Inserimento dei dati su log file
                //System.out.println(R.RecRT.getString());
                Log.WriteLocalization(R.RecRT.getString());
            }
        } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordZ) {
//            InsertZBRecordRT(R);
        } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordE) {
            if (Conf.DisableDBWriteData == 0) {
                InsertZBRecordE(Conn, R);
            } else {
                // TODO Inserimento dei dati su log file
                //System.out.println(R.RecE.E[0].getString());
                Log.WriteEvent(R.RecE.E[0].getString());
            }
        } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordI) {
            if (Conf.DisableDBWriteData == 0) {
                if (R.RecordSubType == 0) {                // info di tipo 1.0
                    InsertZBRecordI(Conn, R);
                } else if (R.RecordSubType == 1) {         // info di configurazione SAFe2.0
                    InsertZBRecordI1(Conn, R);
                } else if (R.RecordSubType == 2) {         // info di tipo 2.0
                    InsertZBRecordI2(Conn, R);
                }
            }

        }
        return 0;
    }
   
    /**
     * Inserisce un record Info I ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordI(Connection Conn, ZBRecord R) throws SQLException {
        String QueryString;
        PreparedStatement statement, statement1;
        ResultSet rs;

        QueryString = "INSERT INTO ZBInfo(IDBlackBox, ErrorsNumber, MaiorVersion, MinorVersion,"
                + " DateVersion, TimeVersion, LastOperativeTime, IDRecord , BTimeStamp, HWRev,PKRandID,"
                + " NFCMaiorVersion,NFCMinorVersion) VALUES ("
                + " ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)";

        statement = DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
        statement.setLong(1, R.IDBlackBox);
        statement.setInt(2, R.RecI.ErrorNumber);
        statement.setInt(3, R.RecI.MaiorVersion);
        statement.setInt(4, R.RecI.MinorVersion);
        statement.setString(5, R.RecI.DateVersion);
        statement.setString(6, R.RecI.TimeVersion);
        statement.setLong(7, R.RecI.LastOperativeTime);
        statement.setLong(8, R.IDRec);
        statement.setTimestamp(9, R.RecordReceivingTime);
        statement.setInt(10, R.RecI.HWRev);
        statement.setInt(11, R.RecI.PKRandID);
        statement.setInt(12, R.RecI.NFC_MaiorVersion);
        statement.setInt(13, R.RecI.NFC_MinorVersion);
        statement.execute();
        ResultSet generatedKeys;
        generatedKeys = statement.getGeneratedKeys();
        if (generatedKeys.next()) {
            long IDZBInfo = generatedKeys.getLong(1);
            generatedKeys.close();
            QueryString = "UPDATE BlackBox SET LastIDZBInfo=? WHERE IDBlackBox=? ";
            statement1 = Conn.prepareStatement(QueryString);
            statement1.setLong(1, IDZBInfo);
            statement1.setLong(2, R.IDBlackBox);
            statement1.execute();
            statement1.close();
        }
        statement.close();

        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
        statement = DbAdminConn.prepareStatement(QueryString);
        statement.setLong(1, R.IDRec);    //IDRec
        statement.execute();
        statement.close();


        // verifica della versione 
        QueryString = "SELECT IDSWVersion, SWVersion, DataRilascio,MaiorVersion, MinorVersion,IDBlackBoxType FROM swversion "
                + "WHERE IDBlackBoxType=2 and MaiorVersion=? and MinorVersion=? "
                + "ORDER BY IDSWVersion desc limit 1";

        statement = DbAdminConn.prepareStatement(QueryString);
        statement.setInt(1, R.RecI.MaiorVersion);
        statement.setInt(2, R.RecI.MinorVersion);
        ResultSet rs1;
        rs1 = statement.executeQuery();
        if (rs1.next()) {
            Log.WriteLog(1, "SW Version " + R.RecI.MaiorVersion + "." + R.RecI.MinorVersion + " ID=" + rs1.getInt("IDSWVersion"));
            QueryString = "UPDATE BlackBox SET IDSWVersion=? WHERE IdBlackBox=?";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, rs1.getInt("IDSWVersion"));    //IDSWVersion
            statement.setLong(2, R.IDBlackBox);    //IDBlackBox
            statement.execute();
            statement.close();
        }

        return 0;
    }

    /**
     * Inserisce un record Info I ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordI2(Connection Conn, ZBRecord R) throws SQLException {
        if (R.RecI != null) {
            InsertZBRecordI(Conn, R);
            return 0;
        }
        String QueryString;
        PreparedStatement statement;
        QueryString = "UPDATE ZBInfo SET IMEI=?, ICCID=? WHERE IDBlackBox=? AND PKRandID=? AND IMEI is null AND ICCID is null";

        statement = DbAdminConn.prepareStatement(QueryString);
        statement.setString(1, R.RecI2.IMEI);
        statement.setString(2, R.RecI2.ICCID);
        statement.setLong(3, R.IDBlackBox);
        statement.setInt(4, R.RecI2.PKRandID);
        statement.execute();
        statement.close();

        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
        statement = DbAdminConn.prepareStatement(QueryString);
        statement.setLong(1, R.IDRec);    //IDRec
        statement.execute();
        statement.close();

        QueryString = "UPDATE BlackBox SET IMEI=? WHERE IdBlackBox=?";
        statement = DbAdminConn.prepareStatement(QueryString);
        statement.setString(1, R.RecI2.IMEI);    //IMEI
        statement.setLong(2, R.IDBlackBox);    //IDBlackBox
        statement.execute();
        statement.close();

        return 0;
    }

    /**
     * Inserisce un record ConfSafe I ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordI1(Connection Conn, ZBRecord R) throws SQLException {
        String QueryString;
        PreparedStatement statement, statement1;

        if (R.RecI1.PkNum == 0) {
            int i;
            QueryString = "INSERT INTO zbconfsafe (IDBlackBox, TagD, TagT, TagS, TagA, TagB, TagW, TagH, TagE,"
                    + " SogliaSpeed, SogliaKM, NumTelMaster, NumTelSlave1 , NumTelSlave2, NumTelSlave3, NumTelSlave4,"
                    + " NumTelSlave5,NumTelSlave6,NumTelSlave7,RandAuth,PKRandID,NumTelConfig) VALUES ("
                    + " ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?, ?, ?, ?, ?, ?, ?, ?,null,?,?)";

            statement = DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, R.IDBlackBox);
            for (i = 0; i < 8; i++) {
                statement.setBoolean(2 + i, R.RecI1.Tags[i]);
            }
            for (i = 0; i < 2; i++) {
                statement.setInt(10 + i, R.RecI1.Soglia[i]);
            }

            statement.setString(12, R.RecI1.NumTelMaster);
            for (i = 0; i < 7; i++) {
                statement.setString(13 + i, R.RecI1.NumTelSlave[i]);
            }
//            statement.setInt(20,R.RecI1.RandAuth);
//            statement.setInt(20,null);
            statement.setInt(20, R.RecI1.PKRandID);
            statement.setInt(21, R.RecI1.NumTelConfig);
            statement.execute();
            ResultSet generatedKeys;
            generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                long IDZBConfSafe = generatedKeys.getLong(1);
                generatedKeys.close();
                QueryString = "UPDATE BlackBox SET LastIDZBConfSafe=? WHERE IDBlackBox=? ";
                statement1 = Conn.prepareStatement(QueryString);
                statement1.setLong(1, IDZBConfSafe);
                statement1.setLong(2, R.IDBlackBox);
                statement1.execute();
                statement1.close();
            }
            statement.close();
        } else {
            int i;
            QueryString = "UPDATE zbconfsafe SET NumTelSlave3=?, NumTelSlave4=?,"
                    + " NumTelSlave5=?,NumTelSlave6=?,NumTelSlave7=?,RandAuth=?, NumTelConfig=? "
                    + "WHERE IDBlackBox=? AND PKRandID=? AND RandAuth is null";

            statement = DbAdminConn.prepareStatement(QueryString);

            for (i = 0; i < 5; i++) {
                statement.setString(i + 1, R.RecI1.NumTelSlave[2 + i]);
            }
            statement.setLong(6, R.RecI1.RandAuth);
            statement.setInt(7, R.RecI1.NumTelConfig);
            statement.setLong(8, R.IDBlackBox);
            statement.setInt(9, R.RecI1.PKRandID);
            statement.execute();
            statement.close();
        }

        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
        statement1 = DbAdminConn.prepareStatement(QueryString);
        statement1.setLong(1, R.IDRec);    //IDRec
        statement1.execute();
        statement1.close();

        return 0;
    }

    /**
     * Inserisce un record Realtime R ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordRT(Connection Conn, ZBRecord R) throws SQLException {
        String QueryString;
        for (int i = 0; i < R.RecRT.NumTracciamenti; i++) {
            PreparedStatement statement1, statement2;
            ResultSet rs;
            QueryString = "select IDZBLocalization,IDBlackBox, BTimeStamp from ZBLocalization where IDBlackBox=? and BTimeStamp=? limit 1";
            statement1 = Conn.prepareStatement(QueryString);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setTimestamp(2, R.RecRT.T[i].Data);
            rs = statement1.executeQuery();
            if (rs.next()) {
                Log.WriteLog(3, "Record RT IDZBLocalization=" + rs.getLong("IDZBLocalization") + " gia presente");
                rs.close();
                statement1.close();

                UpdateZBRecordStato(Conn, R.IDRec, 3);
                continue;
            }
            rs.close();
            statement1.close();
            //System.out.println("setTimestamp "+R.RecRT.T[i].Data);
            QueryString = "INSERT INTO ZBLocalization (IDBlackBox, BLat, BLong, BTimeStamp,FuelLevel, StatoZB,IDRecord,ValidGPS, QualityGPS,ReceiveTimeStamp) VALUES ("
                    + " ?, ?, ?, ?,?,?,?,?,?,CURRENT_TIMESTAMP)";
            statement1 = Conn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setDouble(2, R.RecRT.T[i].Lat);
            statement1.setDouble(3, R.RecRT.T[i].Long);
            statement1.setTimestamp(4, R.RecRT.T[i].Data);
//            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            statement1.setString(4, formatter.format(R.RecRT.T[i].Data));
//            statement1.setLong(4, (long)((R.RecRT.T[i].Data.getTime())/1000));
            statement1.setInt(5, R.RecRT.Fuel);
            statement1.setInt(6, R.RecRT.T[i].StatoZB);
            statement1.setLong(7, R.IDRec);
            statement1.setInt(8, R.RecRT.T[i].ValidGPS); //ValidGPS (Spare Byte 3 - Primo Bit) - Record RealTime
            statement1.setInt(9, R.RecRT.T[i].QualityGPS); //QualityGPS0 - Record RealTime
//            statement1.setTimestamp(10, new java.sql.Timestamp((new java.util.Date()).getTime())); //Receiving Time
            statement1.execute();
            ResultSet generatedKeys;
            generatedKeys = statement1.getGeneratedKeys();
            if (generatedKeys.next()) {
                long IDZBLocalization = generatedKeys.getLong(1);
                generatedKeys.close();
                QueryString = "UPDATE BlackBox SET LastIDZBLocalization=? WHERE IDBlackBox=? ";
                statement2 = Conn.prepareStatement(QueryString);
                statement2.setLong(1, IDZBLocalization);
                statement2.setLong(2, R.IDBlackBox);
                statement2.execute();
                statement2.close();
                Log.WriteLog(3, "Record RT IDZBLocalization=" + IDZBLocalization + " inserito");

                generatedKeys.close();
            }
            statement1.close();
            UpdateZBRecordStato(Conn, R.IDRec, 1);
        }
        return 0;
    }

    /**
     * Inserisce un record Evento R ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordE(Connection Conn, ZBRecord R) throws SQLException {
        //System.out.println(" Record Evento: " + R.RecE.E[0].EventDescr);
        String QueryString;
        PreparedStatement statement1 = null, statement2 = null;
        ResultSet rs = null;
        ResultSet generatedKeys = null;
        for (int i = 0; i < R.RecE.NumEventi; i++) {
            try {
                QueryString = "select IDZBEvents,IDBlackBox, BTimeStamp,IDType,Extra from ZBEvents where IDBlackBox=? and BTimeStamp=? "
                        + " and IDType=? and Extra=? limit 1";
                statement1 = Conn.prepareStatement(QueryString);

                statement1.setLong(1, R.IDBlackBox);
                statement1.setTimestamp(2, R.RecE.E[0].T.Data);
                statement1.setLong(3, R.RecE.E[0].TypeEv);
                statement1.setBytes(4, R.RecE.E[0].Extra);
                rs = statement1.executeQuery();

                if (rs.next()) {
                    Log.WriteLog(3, "Record E IDZBEvents=" + rs.getLong("IDZBEvents") + " gia presente");
                    continue;
                }
                QueryString = "INSERT INTO ZBEvents (IDBlackBox,IDType, BLat, BLong, BTimeStamp,Extra,IDRecord,ReceiveTimeStamp) VALUES ("
                        + " ?, ?, ?, ?,?,?,?,?)";
                statement2 = Conn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                statement2.setLong(1, R.IDBlackBox);
                statement2.setInt(2, R.RecE.E[0].TypeEv);
                statement2.setDouble(3, R.RecE.E[0].T.Lat);
                statement2.setDouble(4, R.RecE.E[0].T.Long);
                statement2.setTimestamp(5, R.RecE.E[0].T.Data);
                statement2.setBytes(6, R.RecE.E[0].Extra);
                statement2.setLong(7, R.IDRec);
                statement2.setTimestamp(8, new java.sql.Timestamp((new java.util.Date()).getTime())); //Receiving Time

                statement2.execute();

                generatedKeys = statement2.getGeneratedKeys();
                if (generatedKeys.next()) {
                    long IDZBEvents = generatedKeys.getLong(1);
                    //                QueryString = "UPDATE BlackBox SET LastIDZBLocalization=? WHERE IDBlackBox=? ";
                    //                statement2 = Conn.prepareStatement(QueryString);
                    //                statement2.setLong(1,IDZBLocalization);
                    //                statement2.setLong(2,R.IDBlackBox);
                    //                statement2.execute();
                    //                statement2.close();
                    Log.WriteLog(3, "Record E IDZBEvents=" + IDZBEvents + " inserito");

                }

                UpdateZBRecordStato(Conn, R.IDRec, 1);
            } finally {
                if (statement1 != null) {
                    statement1.close();
                }
                if (rs != null) {
                    rs.close();
                }
                if (statement2 != null) {
                    statement2.close();
                }
                if (generatedKeys != null) {
                    generatedKeys.close();
                }
            }
        }
        return 0;
    }

    public void UpdateZBRecordStato(Connection Conn, long IDRec, int Stato) throws SQLException {
        if (IDRec != 0) {
            String QueryString = "UPDATE ZBRecords SET Stato=" + Stato + " WHERE IDRec=?";
            PreparedStatement statement1 = Conn.prepareStatement(QueryString);
            statement1.setLong(1, IDRec);    //IDRec
            statement1.execute();
            statement1.close();
        }
    }

//    public Timestamp GetTimeStamp(byte Data[]) {
//        TimeZone tz = TimeZone.getTimeZone("UTC");
//        Calendar c = Calendar.getInstance(tz);
//
//        Timestamp TS;
//        int Year = (uBToI(Data[0]) >> 1) + 2000;
//        int Month = ((uBToI(Data[0]) & 0x1) << 3) + ((uBToI(Data[1]) >> 5) & 0x7);
//        int Day = ((uBToI(Data[1])) & 0x1F);
//        int Hour = (uBToI(Data[2]) >> 3) & 0x1F;
//        int Minutes = ((uBToI(Data[2]) & 0x7) << 3) + ((uBToI(Data[3]) >> 5) & 0x7);
//        int Seconds = ((uBToI(Data[3])) & 0x1F) << 1;
//        c.set(Year, Month - 1, Day, Hour, Minutes, Seconds);
//        TS = new Timestamp(c.getTimeInMillis());
//        if (TS.getYear() + 1900 < 1971) {
//            TS = null;
//        } else if (TS.getYear() + 1900 > 2030) {
//            TS = null;
//        }
//
//        return TS;
//    }

//    public double GetCoord(byte Data[]) {
//        double Coord;
//        int Gradi = uBToI(Data[0]);
//        if ((Data[1] >> 7) == 1) {
//            Gradi = -Gradi;
//        }
//        int Dec = ((uBToI(Data[1]) & 0xF) << 16)
//                + ((uBToI(Data[2]) & 0xFF) << 8)
//                + ((uBToI(Data[3]) & 0xFF));
//
//        Coord = Gradi + (((double) Dec) / 10000);
//
//        return Coord;
//    }

    /**
     * Permette di sapere la presenza di comandi da inviare in coda per la ZB
     *
     * @param IDBlackBox
     * @return il numero di messaggi in coda; 0 se nessun messaggio in coda
     */
    public Byte IsCommandWaiting(int IDBlackBox) {
        int Count = 0;
         Log.WriteLog(1, "1 ricevuto IsCommandWaiting:"+IDBlackBox);
        PreparedStatement statement, statement1;
        ResultSet rs, rs1;

        try {
            String QueryString = "SELECT count(IDZBCommand) as CNT from ZBCommand "
                    + "WHERE IDBlackBox=? and Stato=0";
            
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                Count = rs.getInt(1);
//                Log.WriteLog(3," Conf.TaxiDataCheck="+Conf.TaxiDataCheck+" IDBlackBox="+IDBlackBox);
                if (Conf.TaxiDataCheck > 0) {
                    String QueryString1 = "SELECT count(*) C FROM taxi.tozb where stato=0 and IDBlackBox=?";
                    Log.WriteLog(3, "Conf.TaxiDataCheck=" + Conf.TaxiDataCheck);
                    statement1 = DBTaxi.prepareStatement(QueryString1);
                    statement1.setInt(1, IDBlackBox);
                    rs1 = statement1.executeQuery();
                    if (rs1.next()) {
                        Count += rs1.getInt(1);
                        Log.WriteLog(3, " Conf.TaxiDataCheck=" + Conf.TaxiDataCheck + " IDBlackBox=" + IDBlackBox + " Count=" + Count);
                    }
                    rs1.close();
                    statement1.close();
                }

                if (Count > 254) {
                    Count = 254;
                }
            }
            rs.close();
            statement.close();
        } catch (SQLException Ex) {
            Log.WriteLog(1, "SQL Exception: "+Ex.toString());
            return 0;
        }

        return (byte) Count;
    }

    //--------------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //--------------------------------------------------------
    public Byte IsCommandWaiting(int IDBlackBox, Connection conn) {
        int Count = 0;

        PreparedStatement statement = null, statement1 = null;
        ResultSet rs = null, rs1 = null;

        try {
            String QueryString = "SELECT count(IDZBCommand) as CNT from ZBCommand "
                    + "WHERE IDBlackBox=? and Stato=0";
            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                Count = rs.getInt(1);
//                Log.WriteLog(3," Conf.TaxiDataCheck="+Conf.TaxiDataCheck+" IDBlackBox="+IDBlackBox);
                if (Conf.TaxiDataCheck > 0) {
                    String QueryString1 = "SELECT count(*) C FROM taxi.tozb where stato=0 and IDBlackBox=?";
                    Log.WriteLog(3, "Conf.TaxiDataCheck=" + Conf.TaxiDataCheck);
                    statement1 = conn.prepareStatement(QueryString1);
                    statement1.setInt(1, IDBlackBox);
                    rs1 = statement1.executeQuery();
                    if (rs1.next()) {
                        Count += rs1.getInt(1);
                        Log.WriteLog(3, " Conf.TaxiDataCheck=" + Conf.TaxiDataCheck + " IDBlackBox=" + IDBlackBox + " Count=" + Count);
                    }
                    rs1.close();
                    statement1.close();
                }

                if (Count > 254) {
                    Count = 254;
                }
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            Log.WriteLog(1, "IsCommandWaiting SQL Exception: "+e.toString());
            Log.WriteLog(1, "IsCommandWaitingStack:  :"+Utils.getStackTrace(e));
            return 0;
        } catch (Exception e2){
            Log.WriteLog(1, "IsCommandWaiting Exception: "+e2.toString());
            Log.WriteLog(1, "IsCommandWaitingStack:  :"+Utils.getStackTrace(e2));
            return 0;
        } finally {
            
            try {
                if(rs1!=null)
                    rs1.close();
                if(statement1!=null)
                    statement1.close();
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            } catch (SQLException ex) {
            }
        }

        return (byte) Count;
    }
    //--------------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //--------------------------------------------------------
    public byte[] GetCommandWaiting(int IDBlackBox, Connection conn) {
        byte Command[] = null;

        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            String QueryString = "SELECT IDZBCommand, IDBlackBox, Command, Stato from ZBCommand "
                    + "WHERE IDBlackBox=? and Stato=0 order by IDZBCommand";
            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                Command = rs.getBytes(3);
                long IDZBCommand = rs.getLong("IDZBCommand");
                Log.WriteLog(1, "GetCommandWaiting command:" + Command.toString() + " IDZBCommand=" + rs.getInt("IDZBCommand"));
                rs.close();
                statement.close();

                QueryString = "UPDATE ZBCommand SET LastSent=? WHERE IDZBCommand=?";
                statement = conn.prepareStatement(QueryString);
                statement.setTimestamp(1, new Timestamp((new java.util.Date()).getTime()));
                statement.setLong(2, IDZBCommand);
                statement.executeUpdate();
                //statement.close();
            } 
            //else {
            //    Log.WriteLog(1, "Chiusura.... ");
            //    rs.close();
            //    statement.close();
            //}


        } catch (SQLException e) {
            Log.WriteLog(1, "GetCommandWaiting SQL Exception: "+e.toString());
            Log.WriteLog(1, "GetCommandWaiting:  :"+Utils.getStackTrace(e));
            return null;
        } catch (Exception e2){
            Log.WriteLog(1, "GetCommandWaiting Exception: "+e2.toString());
            Log.WriteLog(1, "GetCommandWaiting:  :"+Utils.getStackTrace(e2));
            return null;
        } finally {
            
            try {
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            } catch (SQLException ex) {
            }
        }

        return Command;
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public boolean SetCommandWaiting(int IDBlackBox, byte Replay[], Connection conn) {
        
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            String QueryString = "SELECT IDZBCommand, IDBlackBox, Command, Stato from ZBCommand "
                    + "WHERE IDBlackBox=? and Stato=0 order by IDZBCommand";
            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                long IDZBCommand = rs.getLong(1);
                QueryString = "UPDATE ZBCommand SET Stato=1,Reply=? "
                        + "WHERE IDZBCommand=?";
                statement = conn.prepareStatement(QueryString);
                statement.setBytes(1, Replay);
                statement.setLong(2, IDZBCommand);
                statement.execute();
                Log.WriteLog(2, "UPDATE ZBCommand SET Stato=1,Reply=" + Replay.toString() + " WHERE IDZBCommand=" + IDZBCommand);
            }
            //rs.close();
            //statement.close();
            return true;
        } catch (SQLException e) {
            Log.WriteLog(1, "SetCommandWaiting SQL Exception: "+e.toString());
            Log.WriteLog(1, "SetCommandWaiting:  :"+Utils.getStackTrace(e));
            return false;
        } catch (Exception e2){
            Log.WriteLog(1, "SetCommandWaiting Exception: "+e2.toString());
            Log.WriteLog(1, "SetCommandWaiting:  :"+Utils.getStackTrace(e2));
            return false;
        } finally {
            
            try {
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            } catch (SQLException ex) {
            }
        }
    }
    
    
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public long AddStartToZBFileUpload(zbox ZB, int filetype, int ChunkTot, int ChunkSize, 
                                String filename, byte CRC[], Connection conn) {
        PreparedStatement statement;
        ResultSet rs;
        long ret = -1;
        /*CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return ret;
            }
        } catch (SQLException ex) {
            return ret;
        }*/
        try {

            // esegue la query al DB
            String QueryString = "select idZBFileUpload, IDBlackBox, FileTimestamp, Filename"
                    + " from ZBFileUpload "
                    + " WHERE IDBlackBox=? and Filename=? AND FileTimestamp>?";

            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setString(2, filename);
            statement.setTimestamp(3, new Timestamp((new java.util.Date()).getTime() - 604800000));  // 7 giorni
            rs = statement.executeQuery();

            if (rs.next()) {
                try {
                    ret = rs.getLong("idZBFileUpload");
                    //System.out.println("Record gia' presente");
                    rs.close();
                    statement.close();
                    QueryString = "UPDATE ZBFileUpload SET Stato=0, Size=0, LastChunk=0,  ChunkTot=?, "
                            + " FileType=? , Hash=?, FileData=? ,FileTimestamp=?, ChunkSize=?"
                            + " WHERE idZBFileUpload=?";

                    statement = conn.prepareStatement(QueryString);
                    statement.setInt(1, ChunkTot);
                    statement.setInt(2, filetype);
                    statement.setBytes(3, CRC);
                    byte zero[] = new byte[ChunkTot * ChunkSize];
                    statement.setBytes(4, zero);
                    statement.setTimestamp(5, new Timestamp((new java.util.Date()).getTime()));
                    statement.setInt(6, ChunkSize);
                    statement.setLong(7, ret);
                    statement.execute();
    //                statement.close();
    //                rs.close();
    //                statement.close();

                    return ret;
                } finally {
                    if (rs != null) { rs.close();}
                    if (statement!= null) { statement.close();}
                }
            }
            rs.close();
            statement.close();

            try {
                QueryString = "INSERT INTO ZBFileUpload (IDBlackBox, FileTimestamp, FileType, Size, Stato, LastChunk, ChunkTot, ChunkSize, Filename, Hash, FileData)"
                        + " VALUES(?,?,?,0,0,0,?,?,?,?,?)";

                statement = conn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                statement.setInt(1, ZB.IDBlackBox);
                statement.setTimestamp(2, new Timestamp((new java.util.Date()).getTime()));
                statement.setInt(3, filetype);
                statement.setInt(4, ChunkTot);
                statement.setInt(5, ChunkSize);
                statement.setString(6, filename);
                statement.setBytes(7, CRC);
                byte zero[] = new byte[ChunkTot * ChunkSize];
                statement.setBytes(8, zero);
                statement.execute();
                ResultSet generatedKeys;
                generatedKeys = statement.getGeneratedKeys();
                if (generatedKeys.next()) {
                    ret = generatedKeys.getLong(1);
                    generatedKeys.close();
                }
            } finally {
                if( statement != null) { statement.close();}
            }
//            DbRTConn.commit();
            return ret;
        } catch (SQLException ex) {
            Log.WriteLog(1, "AddStartToZBFileUpload: "+ex.toString());
            Log.WriteLog(1, "AddStartToZBFileUpload Stacktrace :"+Utils.getStackTrace(ex));
            return ret;
        } catch (Exception e2){
            Log.WriteLog(1, "AddStartToZBFileUpload Exception: "+e2.toString());
            Log.WriteLog(1, "AddStartToZBFileUpload:  :"+Utils.getStackTrace(e2));
            return ret;
        }
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public FileTrasfer UpdateChunkDataToZBFileUpload(zbox ZB, int ByteCount, int ChunkNum, long IdFile, byte chunk[], Connection conn) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        int SizeCurr;
        byte ByteFileData[];
        byte ByteFileDataDest[];
        FileTrasfer FT = null;

        /*CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return FT;
            }
        } catch (SQLException ex) {
            return FT;
        }*/
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "select idZBFileUpload, ChunkTot, ChunkSize, LastChunk, IDBlackBox, FileData"
                    + " from ZBFileUpload "
                    + " WHERE IDBlackBox=? and idZBFileUpload=?";

            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();
            if (rs.next()) {
                if (chunk != null) {
                    System.out.append("Chunk Size=" + chunk.length + " ChunkNum" + ChunkNum+" ");
                }

                FT = new FileTrasfer();
                FT.ChunkTot = rs.getInt("ChunkTot");
                FT.IDFile = IdFile;
                FT.ChunkSize = rs.getInt("ChunkSize");
                SizeCurr = (FT.ChunkSize * ChunkNum) + ByteCount;
                ByteFileData = rs.getBytes("FileData");
                ByteFileDataDest = new byte[SizeCurr];
                if (ByteFileData != null) {
                    System.out.append("ByteFileData Size=" + ByteFileData.length);
                }
                System.arraycopy(ByteFileData, 0, ByteFileDataDest, 0, FT.ChunkSize * ChunkNum);
                System.arraycopy(chunk, 0, ByteFileDataDest, FT.ChunkSize * ChunkNum, ByteCount);

                rs.close();
                statement.close();

                QueryString = "UPDATE ZBFileUpload SET Size=?, LastChunk=?, FileData=? "
                        + " WHERE IDBlackBox=? and idZBFileUpload=?";

                statement = conn.prepareStatement(QueryString);
                statement.setInt(1, SizeCurr);
                statement.setInt(2, ChunkNum);
                statement.setBytes(3, ByteFileDataDest);
                statement.setInt(4, ZB.IDBlackBox);
                statement.setLong(5, IdFile);
                statement.execute();
                statement.close();
//                DbRTConn.commit();
            }
        } catch (SQLException ex) {
            Log.WriteLog(1, "UpdateChunkDataToZBFileUpload: "+ex.toString());
            Log.WriteLog(1, "UpdateChunkDataToZBFileUpload Stacktrace :"+Utils.getStackTrace(ex));
            return null;
        } catch (Exception e2){
            Log.WriteLog(1, "UpdateChunkDataToZBFileUpload Exception: "+e2.toString());
            Log.WriteLog(1, "UpdateChunkDataToZBFileUpload:  :"+Utils.getStackTrace(e2));
            return null;
        } finally {
            
            try {
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            } catch (SQLException ex) {
            }
        }
        return FT;
    }

    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public boolean VerifyHashUploadFile(zbox ZB, long IdFile, Connection conn) {
        byte ByteFileData[] = null;
        byte SHA1Digest[];
        byte hash[] = new byte[20];
        int Size = 0;
        PreparedStatement statement = null;
        ResultSet rs = null;
        MessageDigest md;

        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        try {
            String QueryString = "select idZBFileUpload, Hash, Size, FileData"
                    + " from ZBFileUpload "
                    + " WHERE IDBlackBox=? and idZBFileUpload=? and Stato=0";
            statement = conn.prepareStatement(QueryString);
            statement.setLong(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();
            if (rs.next()) {
                hash = rs.getBytes("Hash");
                Size = rs.getInt("Size");
                ByteFileData = rs.getBytes("FileData");
            }
            rs.close();
            statement.close();
            if (ByteFileData != null) {
                Log.WriteLog(3, "Verifica Hash");
                md.update(ByteFileData, 0, Size);
                SHA1Digest = md.digest();
                if (Arrays.equals(hash, SHA1Digest)) {
                    Log.WriteLog(3, "Hash verificato correttamente");
                    return true;
                }
            }
            Log.WriteLog(3, "Hash NON verificato correttamente");
            return false;
        } catch (SQLException ex) {
            Log.WriteLog(1, "VerifyHashUploadFile: "+ex.toString());
            Log.WriteLog(1, "VerifyHashUploadFile Stacktrace :"+Utils.getStackTrace(ex));
            return false;
        } catch (Exception e2){
            Log.WriteLog(1, "VerifyHashUploadFile Exception: "+e2.toString());
            Log.WriteLog(1, "VerifyHashUploadFile:  :"+Utils.getStackTrace(e2));
            return false;
        } finally {
            
            try {
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            } catch (SQLException ex) {
            }
        }
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public boolean UpdateStatoIntoZBFileUpload(zbox ZB, long IdFile, Connection conn) {
        PreparedStatement statement = null;
        /*CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return false;
            }
        } catch (SQLException ex) {
            return false;
        }*/
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "UPDATE ZBFileUpload SET Stato=1"
                    + " WHERE IDBlackBox=? and idZBFileUpload=? ";

            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            statement.execute();
            statement.close();
//            DbRTConn.commit();
            return true;
        /*} catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            try {
                DbRTConn.rollback();
            } catch (SQLException ex1) {
                Log.WriteEx(DBAdminClass.class.getName(), ex);
                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
            }
            return false;*/
        } catch (SQLException ex) {
            Log.WriteLog(1, "UpdateStatoIntoZBFileUpload: "+ex.toString());
            Log.WriteLog(1, "UpdateStatoIntoZBFileUpload Stacktrace :"+Utils.getStackTrace(ex));
            return false;
        } catch (Exception e2){
            Log.WriteLog(1, "UpdateStatoIntoZBFileUpload Exception: "+e2.toString());
            Log.WriteLog(1, "UpdateStatoIntoZBFileUpload:  :"+Utils.getStackTrace(e2));
            return false;
        } finally {
            
            try{
                if (statement!=null)
                    statement.close();
            }catch (SQLException ex) {
            }
        }
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public DownloadInfo ReadDownloadInfo(zbox ZB, Connection conn) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        DownloadInfo DwnlInfo = null;
        /*CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return DwnlInfo;
            }
        } catch (SQLException ex) {
            return DwnlInfo;
        }*/
        try {
//            DbRTConn.setAutoCommit(false);
            // esegue la query al DB
            String QueryString = "SELECT DS.idzbfiledownloadsession, DS.FilePathOnZBox, FD.FileDownloadSize,  "
                    + "FD.FileDownloadType from zbfiledownloadsession DS "
                    + "LEFT JOIN blackbox BB ON BB.IdBlackBox=DS.IdBlackBox "
                    + "LEFT JOIN zbfiledownload FD ON FD.Idzbfiledownload=DS.Idzbfiledownload "
                    + "WHERE DS.Stato=0 and BB.IdBlackBox=? "
                    + "ORDER BY DS.idzbfiledownloadsession Desc";

            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, ZB.IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                DwnlInfo = new DownloadInfo();
                DwnlInfo.FileType = rs.getInt("FD.FileDownloadType");
                DwnlInfo.FileSize = rs.getLong("FD.FileDownloadSize");
                DwnlInfo.PathFileName = rs.getString("DS.FilePathOnZBox");
                DwnlInfo.IDFile = rs.getLong("DS.idzbfiledownloadsession");
//                DbRTConn.commit();
            }
            //rs.close();
            //statement.close();
        } catch (SQLException ex) {
            Log.WriteLog(1, "ReadDownloadInfo: "+ex.toString());
            Log.WriteLog(1, "ReadDownloadInfo Stacktrace :"+Utils.getStackTrace(ex));
            return null;
        } catch (Exception e2){
            Log.WriteLog(1, "ReadDownloadInfo Exception: "+e2.toString());
            Log.WriteLog(1, "ReadDownloadInfo:  :"+Utils.getStackTrace(e2));
            return null;
        } finally {
            
            try{
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            }catch (SQLException ex) {
            }
        }
        
        return DwnlInfo;
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public DownloadChunkData ReadDownloadChunkData(zbox ZB, FileTrasfer FT, Connection conn) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        DownloadChunkData DwnlChkData = null;
        byte ByteData[];
        /*CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return DwnlChkData;
            }
        } catch (SQLException ex) {
            return DwnlChkData;
        }*/
        try {
//            DbRTConn.setAutoCommit(false);
            //TODO: Per forza doppia query? Sembra di no, prendo il dato passato
            //Leggo l'ultimo chunck inviato e che è stato aggiornato in UpdateDataToZBFileDownloadSession 
            /*
             String QueryStringLastChunk = "SELECT DS.LastSentChunk from zbfiledownloadsession DS "
             + "LEFT JOIN blackbox BB ON BB.IdBlackBox=DS.IdBlackBox "
             + "LEFT JOIN zbfiledownload FD ON FD.Idzbfiledownload=DS.Idzbfiledownload "
             + "WHERE DS.Stato=0 and BB.IdBlackBox=? "
             + "ORDER BY DS.idzbfiledownloadsession ";
            
             statement = DbRTConn.prepareStatement(QueryStringLastChunk);
             statement.setInt(1, ZB.IDBlackBox);
            
             rs = statement.executeQuery();
             if (rs.next()) {
             //Salva ultimo chunk letto
             int LastChunck = rs.getInt("DS.LastSentChunk");
             }
            
             rs.close();
             statement.close();
             */

            //ATTENZIONE: FT.ChunkTot non è popolato il pacchetto FileChunckReq non ha ChunckTot
            String QueryString;
            //Calcola il residuo per la query
//            Log.WriteLog(1,"ChunckCurr/ChunckTot: "+FT.ChunkCurr+"/"+FT.ChunkTot);
            //Mi trovo nell'ultimo Chunck?

            //Capisco prima quale ultimo chunk è stato inviato           
            //esegue la query al DB
            //ESP: la query è stata modificata in modo da ritornare ChunckCurr da FileTransfer che mi fornisce la chiamata della ZBOX 
            QueryString = "SELECT DS.idzbfiledownloadsession, DS.LastSentChunk, FD.FileDownloadSize, ceil(FD.FileDownloadSize/" + FT.ChunkSize + ") TotChunk,"
                    + "SUBSTRING(FD.FileDownloadContent," + Integer.toString((FT.ChunkCurr * FT.ChunkSize) + 1)
                    + ", IF (((" + FT.ChunkCurr + "+1)*" + FT.ChunkSize + ")>FD.FileDownloadSize, FD.FileDownloadSize MOD " + FT.ChunkSize + ", " + FT.ChunkSize + ")) DataChunk from zbfiledownloadsession DS "
                    + "LEFT JOIN blackbox BB ON BB.IdBlackBox=DS.IdBlackBox "
                    + "LEFT JOIN zbfiledownload FD ON FD.Idzbfiledownload=DS.Idzbfiledownload "
                    + "WHERE DS.Stato=0 and BB.IdBlackBox=? "
                    + "ORDER BY DS.idzbfiledownloadsession Desc";

            statement = conn.prepareStatement(QueryString);
            //Utilizzare il set statement
            //statement.setInt(1, FT.ChunkCurr);
            statement.setInt(1, ZB.IDBlackBox);
            //Log Query
            Log.WriteLog(5, "Query for ChunckCurr: " + QueryString + " for BlackBox: " + Integer.toString(ZB.IDBlackBox));
            //Send Query
            rs = statement.executeQuery();
            if (rs.next()) {
                DwnlChkData = new DownloadChunkData();
                DwnlChkData.IDFile = rs.getLong("DS.idzbfiledownloadsession");
//                DwnlChkData.ChunkNum = rs.getInt("DS.LastSentChunk");
                DwnlChkData.ChunkNum = FT.ChunkCurr;
                FT.ChunkTot = rs.getInt("TotChunk");
                if (FT.ChunkCurr > FT.ChunkTot) {
                    Log.WriteLog(1, "Errore: IDBB " + Integer.toString(ZB.IDBlackBox) + " Chunk " + FT.ChunkCurr + "/" + FT.ChunkTot);
                    return null;  // pacchetto richiesto non valido
                }
                int SizeTot = rs.getInt("FD.FileDownloadSize");
                //ByteData = rs.getBytes("SUBSTRING(FD.FileDownloadContent,"+ Integer.toString((FT.ChunkCurr)+1) +",1000)");
                ByteData = rs.getBytes("DataChunk"); //Prendo la quarta colonna ovvero il risultato del quarto campo della SELECT precedente
                //Log
                Log.WriteLog(1, "Transfer ChunkNumber: " + Integer.toString(FT.ChunkCurr));

                //Calcola il residuo
                if ((FT.ChunkSize * (DwnlChkData.ChunkNum + 1)) > SizeTot) {
                    DwnlChkData.ByteCount = (SizeTot % FT.ChunkSize);
                } else {
                    DwnlChkData.ByteCount = FT.ChunkSize;
                }

                DwnlChkData.ByteCount = ByteData.length;

                Log.WriteLog(1, "ChunckCurr/ChunckTot: " + FT.ChunkCurr + "/" + FT.ChunkTot + " IDZB:" + ZB.IDBlackBox);
                Log.WriteLog(1, "DwnlChkData.ByteCount: " + DwnlChkData.ByteCount);
                Log.WriteLog(1, "ByteData Lenght: " + ByteData.length);
                Log.WriteLog(1, "DwnlChkData.ChunkNum: " + DwnlChkData.ChunkNum);
                Log.WriteLog(5, "ByteArray ByteData: " + Arrays.toString(ByteData));
                //System.arraycopy(ByteData, 1000*DwnlChkData.ChunkNum, DwnlChkData.ChunkData, 0, DwnlChkData.ByteCount);
                //ESP 5_06_2013 (Trasferisco sempre 1000 byte, tranne nell'ultimo chunck dove trasferisco il residuo)
                //ES. 352.644 = 352 * 1000 + 644
                //Quindi vengono spediti 644 byte con padding zero dei dati non necessari
                System.arraycopy(ByteData, 0, DwnlChkData.ChunkData, 0, ByteData.length);
                //rs.close();
                //statement.close();
//                DbRTConn.commit();
            }
       
        } catch (SQLException ex) {
            Log.WriteLog(1, "ReadDownloadChunkData: "+ex.toString());
            Log.WriteLog(1, "ReadDownloadChunkData Stacktrace :"+Utils.getStackTrace(ex));
            return null;
        } catch (Exception e2){
            Log.WriteLog(1, "ReadDownloadChunkData Exception: "+e2.toString());
            Log.WriteLog(1, "ReadDownloadChunkData:  :"+Utils.getStackTrace(e2));
            return null;
        } finally {
            
            try{
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            }catch (SQLException ex) {
            }
        }
        return DwnlChkData;
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public void UpdateDataToZBFileDownloadSession(zbox ZB, int ChunkNum, long IdFile, int Stato, Connection conn) {
        PreparedStatement statement = null;

        /*CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return;
            }
        } catch (SQLException ex) {
            return;
        }*/
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "UPDATE ZBFileDownloadSession SET Stato=?, LastSentChunk=?, LastSent_Date=? "
                    + " WHERE IDBlackBox=? and idZBFileDownloadSession=?";

            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, Stato);
            statement.setInt(2, ChunkNum);
            statement.setTimestamp(3, new Timestamp(new java.util.Date().getTime()));
            statement.setInt(4, ZB.IDBlackBox);
            statement.setLong(5, IdFile);
            Log.WriteLog(3, "Update Chunk numero: " + ChunkNum + " IdFile: " + IdFile);
            statement.execute();
            //statement.close();

        } catch (SQLException ex) {
            Log.WriteLog(1, "UpdateDataToZBFileDownloadSession: "+ex.toString());
            Log.WriteLog(1, "UpdateDataToZBFileDownloadSession Stacktrace :"+Utils.getStackTrace(ex));
        } catch (Exception e2){
            Log.WriteLog(1, "UpdateDataToZBFileDownloadSession Exception: "+e2.toString());
            Log.WriteLog(1, "UpdateDataToZBFileDownloadSession:  :"+Utils.getStackTrace(e2));
        } finally {
            
            try{

                if (statement!=null)
                    statement.close();
            }catch (SQLException ex) {
            }
        }
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public boolean VerifyHashDownloadFile(zbox ZB, long IdFile, byte Hash[], Connection conn) {
        byte ByteFileData[] = null;
        byte SHA1Digest[];
        int Size = 0;
        PreparedStatement statement = null;
        ResultSet rs = null;
        MessageDigest md;

        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        try {
            String QueryString = "select FD.idZBFileDownload, FD.FileDownloadSize, FD.FileDownloadContent"
                    + " from ZBFileDownload FD "
                    + " LEFT JOIN zbfiledownloadsession DS ON FD.Idzbfiledownload=DS.Idzbfiledownload "
                    + " WHERE IDBlackBox=? and DS.Idzbfiledownloadsession=?";
            statement = conn.prepareStatement(QueryString);
            statement.setLong(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();
            if (rs.next()) {
                Size = rs.getInt("FD.FileDownloadSize");
                ByteFileData = rs.getBytes("FD.FileDownloadContent");
            }
            rs.close();
            statement.close();
            if (ByteFileData != null) {
                Log.WriteLog(3, "Verifica Hash");
                md.update(ByteFileData, 0, Size);
                SHA1Digest = md.digest();
                //System.out.println("Hash "+Utils.toHexString(Hash));
                //System.out.println("SHA1Digest " +Utils.toHexString(SHA1Digest));

                if (Arrays.equals(Hash, SHA1Digest)) {
                        
                    Log.WriteLog(3, "Hash verificato correttamente");
                    //System.out.println("Hash verificato correttamente");
                    return true;
                }
            }
            Log.WriteLog(3, "Hash NON verificato correttamente");
            //System.out.println("Hash NON verificato correttamente");
            return false;

        } catch (SQLException ex) {
            Log.WriteLog(1, "VerifyHashDownloadFile: "+ex.toString());
            Log.WriteLog(1, "VerifyHashDownloadFile Stacktrace :"+Utils.getStackTrace(ex));
            return false;
        } catch (Exception e2){
            Log.WriteLog(1, "VerifyHashDownloadFile Exception: "+e2.toString());
            Log.WriteLog(1, "VerifyHashDownloadFile:  :"+Utils.getStackTrace(e2));
            return false;
        } finally {
            
            try{
                if(rs!=null)
                    rs.close();
                if (statement!=null)
                    statement.close();
            }catch (SQLException ex) {
            }
        }
    }
    
    //-----------------------------------------------------
    // Metodo modificato: parametro aggiunto la connessione
    //                    migliorata gestione exception
    //-----------------------------------------------------
    public void FinalUpdateDataToZBFileDownloadSession(zbox ZB, long IdFile, int Stato, Connection conn) {
        PreparedStatement statement = null;

        /*CheckConnection();
        try {
            if (DbRTConn.isClosed()) {
                return;
            }
        } catch (SQLException ex) {
            return;
        }*/
        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString = "UPDATE ZBFileDownloadSession SET Stato=? "
                    + " WHERE IDBlackBox=? and idZBFileDownloadSession=?";

            statement = conn.prepareStatement(QueryString);
            statement.setInt(1, Stato);
            statement.setInt(2, ZB.IDBlackBox);
            statement.setLong(3, IdFile);
            statement.execute();
            //statement.close();

        } catch (SQLException ex) {
            Log.WriteLog(1, "FinalUpdateDataToZBFileDownloadSession: "+ex.toString());
            Log.WriteLog(1, "FinalUpdateDataToZBFileDownloadSession Stacktrace :"+Utils.getStackTrace(ex));
            return;
        } catch (Exception e2){
            Log.WriteLog(1, "FinalUpdateDataToZBFileDownloadSession Exception: "+e2.toString());
            Log.WriteLog(1, "FinalUpdateDataToZBFileDownloadSession:  :"+Utils.getStackTrace(e2));
            return;
        } finally {
            
            try{

                if (statement!=null)
                    statement.close();
            }catch (SQLException ex) {
            }
        }
    }
    
    
    public byte[] GetCommandWaiting(int IDBlackBox) {
        byte Command[] = null;

        PreparedStatement statement;
        ResultSet rs;

        try {
            String QueryString = "SELECT IDZBCommand, IDBlackBox, Command, Stato from ZBCommand "
                    + "WHERE IDBlackBox=? and Stato=0 order by IDZBCommand";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                Command = rs.getBytes(3);
                long IDZBCommand = rs.getLong("IDZBCommand");
                Log.WriteLog(1, "GetCommandWaiting command:" + Command.toString() + " IDZBCommand=" + rs.getInt("IDZBCommand"));
                rs.close();
                statement.close();

                QueryString = "UPDATE ZBCommand SET LastSent=? WHERE IDZBCommand=?";
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setTimestamp(1, new Timestamp((new java.util.Date()).getTime()));
                statement.setLong(2, IDZBCommand);
                statement.executeUpdate();
                statement.close();
            } else {
                Log.WriteLog(1, "Chiusura.... ");
                rs.close();
                statement.close();
            }


        } catch (SQLException Ex) {
            Log.WriteLog(1, "GetCommandWaiting serverSocket Exception: "+Ex.toString());
            //Log.WriteEx(DBAdminClass.class.getName(), Ex);
            return null;
        }

        return Command;
    }

    public boolean SetCommandWaiting(int IDBlackBox, byte Replay[]) {
        PreparedStatement statement;
        ResultSet rs;

        try {
            String QueryString = "SELECT IDZBCommand, IDBlackBox, Command, Stato from ZBCommand "
                    + "WHERE IDBlackBox=? and Stato=0 order by IDZBCommand";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, IDBlackBox);
            rs = statement.executeQuery();
            if (rs.next()) {
                long IDZBCommand = rs.getLong(1);
                QueryString = "UPDATE ZBCommand SET Stato=1,Reply=? "
                        + "WHERE IDZBCommand=?";
                statement = DbAdminConn.prepareStatement(QueryString);
                statement.setBytes(1, Replay);
                statement.setLong(2, IDZBCommand);
                statement.execute();
                Log.WriteLog(2, "UPDATE ZBCommand SET Stato=1,Reply=" + Replay.toString() + " WHERE IDZBCommand=" + IDZBCommand);
            }
            rs.close();
            statement.close();
            return true;
        } catch (SQLException Ex) {
            Log.WriteEx(DBAdminClass.class.getName(), Ex);
            return false;
        }
    }


    public boolean VerifyHashUploadFile(zbox ZB, long IdFile) {
        byte ByteFileData[] = null;
        byte SHA1Digest[];
        byte hash[] = new byte[20];
        int Size = 0;
        PreparedStatement statement;
        ResultSet rs;
        MessageDigest md;

        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        try {
            String QueryString = "select idZBFileUpload, Hash, Size, FileData"
                    + " from ZBFileUpload "
                    + " WHERE IDBlackBox=? and idZBFileUpload=? and Stato=0";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setLong(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();
            if (rs.next()) {
                hash = rs.getBytes("Hash");
                Size = rs.getInt("Size");
                ByteFileData = rs.getBytes("FileData");
            }
            rs.close();
            statement.close();
            if (ByteFileData != null) {
                Log.WriteLog(3, "Verifica Hash");
                md.update(ByteFileData, 0, Size);
                SHA1Digest = md.digest();
                if (Arrays.equals(hash, SHA1Digest)) {
                    Log.WriteLog(3, "Hash verificato correttamente");
                    return true;
                }
            }
            Log.WriteLog(3, "Hash NON verificato correttamente");
            return false;
        } catch (SQLException Ex) {
            Log.WriteEx(DBAdminClass.class.getName(), Ex);
            return false;
        }
    }

    public boolean VerifyHashDownloadFile(zbox ZB, long IdFile, byte Hash[]) {
        byte ByteFileData[] = null;
        byte SHA1Digest[];
        int Size = 0;
        PreparedStatement statement;
        ResultSet rs;
        MessageDigest md;

        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        try {
            String QueryString = "select FD.idZBFileDownload, FD.FileDownloadSize, FD.FileDownloadContent"
                    + " from ZBFileDownload FD "
                    + " LEFT JOIN zbfiledownloadsession DS ON FD.Idzbfiledownload=DS.Idzbfiledownload "
                    + " WHERE IDBlackBox=? and DS.Idzbfiledownloadsession=?";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setLong(1, ZB.IDBlackBox);
            statement.setLong(2, IdFile);
            rs = statement.executeQuery();
            if (rs.next()) {
                Size = rs.getInt("FD.FileDownloadSize");
                ByteFileData = rs.getBytes("FD.FileDownloadContent");
            }
            rs.close();
            statement.close();
            if (ByteFileData != null) {
                Log.WriteLog(3, "Verifica Hash");
                md.update(ByteFileData, 0, Size);
                SHA1Digest = md.digest();
                System.out.println("Hash "+Utils.toHexString(Hash));
                System.out.println("SHA1Digest " +Utils.toHexString(SHA1Digest));

                if (Arrays.equals(Hash, SHA1Digest)) {
                        
                    Log.WriteLog(3, "Hash verificato correttamente");
                    System.out.println("Hash verificato correttamente");
                    return true;
                }
            }
            Log.WriteLog(3, "Hash NON verificato correttamente");
            System.out.println("Hash NON verificato correttamente");
            return false;
        } catch (SQLException Ex) {
            Log.WriteEx(DBAdminClass.class.getName(), Ex);
            return false;
        }
    }

    public Token FindIDToken(byte[] SNToken, int IDAzienda) {
        PreparedStatement statement;
        ResultSet rs;
        Token TK = new Token();
        TK.IDAzienda = IDAzienda;
        TK.SetTokenHexString((new String(Hex.encode(SNToken))).toUpperCase());

        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return TK;
            }
        } catch (SQLException ex) {
            return TK;
        }
        try {
            // esegue la query al DB
            String QueryString = "select IDToken, IDAzienda, Cognome, Nome, TokenHWID, TokenHWID_N, IDTokenType, CertidriveIDConducente"
                    + " from Token WHERE TokenHWID='" + (new String(Hex.encode(SNToken))).toUpperCase() + "'"
                    + "     AND IDAzienda=? AND IDTokenType=2;";

            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setInt(1, IDAzienda);
            rs = statement.executeQuery();
            if (rs.next()) {
                TK = new Token();
                TK.IDToken = rs.getInt("IDToken");
                TK.Cognome = rs.getString("Cognome");
                TK.Nome = rs.getString("Nome");
                TK.CDIDconducente = rs.getInt("CertidriveIDConducente");
                TK.SetTokenHexString(rs.getString("TokenHWID"));
                TK.TokenHWID_N = rs.getLong("TokenHWID_N");
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return TK;
    }

    public long InsertToken(Connection Conn, Token TK) {
        PreparedStatement statement;
        ResultSet rs;

        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            String QueryString = "INSERT INTO Token (IDAzienda, TokenHWID, IDTokenType, CertSerialNum, NumProgress,TokenHWID_N)"
                    + " VALUES (?,?,2,0,0,? )";

            statement = DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement.setInt(1, TK.IDAzienda);
            statement.setString(2, TK.GetTokenHexString());
            statement.setLong(3, TK.TokenHWID_N);
            statement.execute();
            ResultSet generatedKeys;
            generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                TK.IDToken = generatedKeys.getLong(1);
                generatedKeys.close();
            }
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return TK.IDToken;
    }

    /**
     * Verifica la presenza di cambiamenti da apportare sulle ZBox riguardo le
     * abilitazioni alla guida dei Conducenti.
     *
     * @return 0 se nessun record cambiato -1 se si è verificato un errore x il
     * numero di record cambiati
     */
    public int CertidriveCheckChanges() {
        int RecChanged = 0;
        PreparedStatement statement, statement1;
        Statement Stm2;
        ResultSet rs, rs1;
        String QueryUpdate;

        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return -1;
            }
        } catch (SQLException ex) {
            return -1;
        }
        try {
            ArrayList<Integer> ZBChangeList = new ArrayList<Integer>();
            // esegue la query al DB
            String QueryString = "select IDChanges, IDToken, IDBlackBox, Stato from Changes \n\r"
                    + " WHERE Stato=0 AND (IDToken IS NOT NULL OR IDBlackBox IS NOT NULL)";

            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();

            while (rs.next()) {
                Stm2 = DbAdminConn.createStatement();
                QueryUpdate = "";
                if (rs.getInt("IDToken") != 0) {
                    Token TK = GetToken(rs.getInt("IDToken"));
                    Azienda Az = GetAzienda(TK.IDAzienda);
                    CDDdAdminClass.GetConducentiList(Az.CertidriveIDAzienda, TK);
                    if (TK.SubNet == null) {
                        Stm2.addBatch("UPDATE Token SET CertidriveIDConducente=null WHERE IDToken=" + TK.IDToken + ";\r\n");
                        Stm2.addBatch("DELETE FROM ZBDriver  WHERE IDToken=" + TK.IDToken + ";\r\n");
                        QueryUpdate += "UPDATE Token SET CertidriveIDConducente=null WHERE IDToken=" + TK.IDToken + ";\r\n";
                        QueryUpdate += "DELETE FROM ZBDriver  WHERE IDToken=" + TK.IDToken + ";\r\n";

                        QueryString = "SELECT IDZBDriver, IDBlackBox, IDToken FROM ZBDriver WHERE IDToken=" + TK.IDToken;
                        statement1 = DbAdminConn.prepareStatement(QueryString);
                        rs1 = statement1.executeQuery();
                        while (rs1.next()) {
                            ZBChangeList.add(rs.getInt("IDBlackBox"));
                        }
                        rs1.close();
                        statement1.close();
                    } else {
                        Stm2.addBatch("UPDATE Token SET CertidriveIDConducente=" + TK.CertidriveIDAzienda + " WHERE IDToken=" + TK.IDToken + ";\r\n");
                        QueryUpdate += "UPDATE Token SET CertidriveIDConducente=" + TK.CertidriveIDAzienda + " WHERE IDToken=" + TK.IDToken + ";\r\n";
                        ArrayList<Integer> IDVeicoli = CDDdAdminClass.GetVeicoliListSubnet(Az.CertidriveIDAzienda, TK.SubNet);
                        Iterator<Integer> itr = IDVeicoli.iterator();
                        while (itr.hasNext()) {
                            QueryString = "SELECT IDBlackBox, IDAzienda, CertidriveIDVeicolo FROM BlackBox "
                                    + "WHERE IDAzienda=" + Az.IDAzienda + " AND CertidriveIDVeicolo=" + itr.next();
                            statement1 = DbAdminConn.prepareStatement(QueryString);
                            rs1 = statement1.executeQuery();
                            if (rs1.next()) {
                                int IDBlackBox = rs1.getInt("IDBlackBox");
                                Stm2.addBatch("INSERT INTO ZBDriver (IDBlackBox, IDToken) "
                                        + "SELECT " + IDBlackBox + " ID1," + TK.IDToken + " ID2 FROM BlackBox WHERE NOT EXISTS "
                                        + "(SELECT IDZBDriver FROM ZBDriver WHERE IDBlackBox=" + IDBlackBox + " AND IDToken=" + TK.IDToken + ") LIMIT 1;\r\n");
                                QueryUpdate += "INSERT INTO ZBDriver (IDBlackBox, IDToken) "
                                        + "SELECT " + IDBlackBox + " ID1," + TK.IDToken + " ID2 FROM BlackBox WHERE NOT EXISTS "
                                        + "(SELECT IDZBDriver FROM ZBDriver WHERE IDBlackBox=" + IDBlackBox + " AND IDToken=" + TK.IDToken + ") LIMIT 1;\r\n";
                                ZBChangeList.add(IDBlackBox);
                            }
                            rs1.close();
                            statement1.close();
                        }

                    }
                }
                if (rs.getInt("IDBlackBox") != 0) {
                    zbox zb = GetZBox(rs.getInt("IDBlackBox"));
                    if (zb != null) {
                        Azienda Az = GetAzienda(zb.IDAzienda);
                        CDDdAdminClass.GetVeicoliList(Az.CertidriveIDAzienda, zb);
                        if (zb.SubNet == null) {
                            Stm2.addBatch("UPDATE blackBox SET CertidriveIDVeicolo=null WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n");
                            Stm2.addBatch("DELETE FROM ZBDriver  WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n");
                            QueryUpdate += "UPDATE blackBox SET CertidriveIDVeicolo=null WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n";
                            QueryUpdate += "DELETE FROM ZBDriver  WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n";
                            ZBChangeList.add(zb.IDBlackBox);

                        } else {
                            Stm2.addBatch("UPDATE blackBox SET CertidriveIDVeicolo=" + zb.CertidriveIDVeicolo + " WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n");
                            QueryUpdate += "UPDATE blackBox SET CertidriveIDVeicolo=" + zb.CertidriveIDVeicolo + " WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n";
                            ArrayList<Integer> IDConducenti = CDDdAdminClass.GetConducentiListSubnet(Az.CertidriveIDAzienda, zb.SubNet);
                            Iterator<Integer> itr = IDConducenti.iterator();
                            while (itr.hasNext()) {
                                QueryString = "SELECT IDToken, IDAzienda, CertidriveIDConducente FROM Token "
                                        + "WHERE IDAzienda=" + Az.IDAzienda + " AND CertidriveIDConducente=" + itr.next();
                                statement1 = DbAdminConn.prepareStatement(QueryString);
                                rs1 = statement1.executeQuery();
                                if (rs1.next()) {
                                    int IDToken = rs1.getInt("IDToken");
                                    Stm2.addBatch("INSERT INTO ZBDriver (IDBlackBox, IDToken) "
                                            + "SELECT " + zb.IDBlackBox + " ID1," + IDToken + " ID2 FROM BlackBox WHERE NOT EXISTS "
                                            + "(SELECT IDZBDriver FROM ZBDriver WHERE IDBlackBox=" + zb.IDBlackBox + " AND IDToken=" + IDToken + ") LIMIT 1;\r\n");
                                    QueryUpdate += "INSERT INTO ZBDriver (IDBlackBox, IDToken) "
                                            + "SELECT " + zb.IDBlackBox + " ID1," + IDToken + " ID2 FROM BlackBox WHERE NOT EXISTS "
                                            + "(SELECT IDZBDriver FROM ZBDriver WHERE IDBlackBox=" + zb.IDBlackBox + " AND IDToken=" + IDToken + ") LIMIT 1;\r\n";
                                }
                                rs1.close();
                                statement1.close();
                            }
                            ZBChangeList.add(zb.IDBlackBox);
                        }
                    }
                }
                Stm2.addBatch("UPDATE Changes SET Stato=1 WHERE IDChanges=" + rs.getInt("IDChanges"));
                QueryUpdate += "UPDATE Changes SET Stato=1 WHERE IDChanges=" + rs.getInt("IDChanges");
//                statement1 = DbAdminConn.prepareStatement(QueryUpdate);
//                statement1.setInt(1,rs.getInt("IDChanges"));    //IDChanges
//                statement1.execute();
//                statement1.close();
                //System.out.println(QueryUpdate);
                Stm2.executeBatch();
                Stm2.close();
                RecChanged++;
            }

            rs.close();
            statement.close();
            Iterator<Integer> itr = ZBChangeList.iterator();
            while (itr.hasNext()) {
                int IDBlackBox = itr.next();
                zbox ZB = GetZBox(IDBlackBox);
                UpdateZBDriver(ZB);
            }
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return RecChanged;

    }

    /**
     * Ripristina i campi CertidriveIDConducente della tabella Token e
     * CertidriveIDVeicolo della tabella BlackBox
     *
     *
     * @return 0 se nessun record cambiato -1 se si è verificato un errore x il
     * numero di record cambiati
     */
    public int CertidriveRestoreIDs() {
        int RecChanged = 0;
        PreparedStatement statement, statement1;
        Statement Stm2;
        ResultSet rs, rs1;
        String QueryUpdate;

        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return -1;
            }
        } catch (SQLException ex) {
            return -1;
        }
        try {
            // esegue la query al DB
            String QueryString = "SELECT A.*,BB.* FROM blackbox BB left join azienda A on BB.IDAzienda=A.IDAzienda"
                    + " where A.CertidriveIDAzienda is not null and BB.CertidriveIDVeicolo is null;";

            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();
            Stm2 = DbAdminConn.createStatement();
            QueryUpdate = "";
            while (rs.next()) {
               //System.out.println("IDBlackBox=" + rs.getInt("IDBlackBox"));
                if (rs.getInt("IDBlackBox") != 0) {
                    zbox zb = GetZBox(rs.getInt("IDBlackBox"));
                    if (zb != null) {
                        Azienda Az = GetAzienda(zb.IDAzienda);
                        CDDdAdminClass.GetVeicoliList(Az.CertidriveIDAzienda, zb);
                        if (zb.CertidriveIDVeicolo >= 0) {
                            Stm2.addBatch("UPDATE blackBox SET CertidriveIDVeicolo=" + zb.CertidriveIDVeicolo + " WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n");
                            QueryUpdate += "UPDATE blackBox SET CertidriveIDVeicolo=" + zb.CertidriveIDVeicolo + " WHERE IDBlackBox=" + zb.IDBlackBox + ";\r\n";
                        }
                    }
                }

            }
            rs.close();
            statement.close();


            QueryString = "SELECT A.*,T.* FROM Token T left join azienda A on T.IDAzienda=A.IDAzienda"
                    + " where A.CertidriveIDAzienda is not null and T.CertidriveIDConducente is null";

            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();

            while (rs.next()) {
                if (rs.getInt("IDToken") != 0) {
                    Token TK = GetToken(rs.getInt("IDToken"));
                    Azienda Az = GetAzienda(TK.IDAzienda);
                    CDDdAdminClass.GetConducentiList(Az.CertidriveIDAzienda, TK);
                    Stm2.addBatch("UPDATE Token SET CertidriveIDConducente=" + TK.CertidriveIDAzienda + " WHERE IDToken=" + TK.IDToken + ";\r\n");
                    QueryUpdate += "UPDATE Token SET CertidriveIDConducente=" + TK.CertidriveIDAzienda + " WHERE IDToken=" + TK.IDToken + ";\r\n";
                }
            }
            rs.close();
            statement.close();

            //System.out.println(QueryUpdate);
            Stm2.executeBatch();
            Stm2.close();

        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return RecChanged;

    }

    public int CertidriveCheckRecordsZXY() {
        PreparedStatement statement, statement1;
        ResultSet rs;

        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return -1;
            }
        } catch (SQLException ex) {
            return -1;
        }
        try {
            Thread.sleep(61000);
        } catch (InterruptedException ex) {
            Logger.getLogger(ZB_ElaboraDati.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            // esegue la query al DB
            String QueryString = "SELECT RZ.IDZBRecZ,RZ.IDBlackBox,RZ.stato,RZ.CertidriveIDGuida, RZ.IDGuida,"
                    + " RZ.BTimeStamp, RZ.FuelLevel, BB.IDAzienda, AA.CertidriveIDAzienda, TK.CertidriveIDConducente"
                    + " FROM zbrecz RZ"
                    + " LEFT JOIN BlackBox BB ON RZ.IDBlackBox= BB.IDBlackBox"
                    + " LEFT JOIN Azienda AA ON BB.IDAzienda=AA.IDAzienda"
                    + " LEFT JOIN Token TK ON TK.IDToken=RZ.IDToken"
                    + " WHERE RZ.stato=0 and AA.CertidriveIDAzienda is not null order by IDZBRecZ limit 500 ";

            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();
            while (rs.next()) {
                int CertidriveIDGuida = -1;
                if (rs.getInt("CertidriveIDGuida") > 0) {
                    // esegue Update del record Z nel DB di Certidrive
                    CertidriveIDGuida = rs.getInt("CertidriveIDGuida");
                    try {
                        int ret = UpdateCertidriveGuida(rs.getInt("IDBlackBox"), rs.getLong("IDZBRecZ"),
                                rs.getInt("CertidriveIDConducente"),
                                rs.getInt("IDGuida"), CertidriveIDGuida,
                                rs.getTimestamp("BTimeStamp"), rs.getInt("FuelLevel"));
                        if (ret == 0) {
                            CertidriveIDGuida = -1;
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    // esegue insert di un nuovo record di guida nel DB di Certidrive
                    CertidriveIDGuida = InsertCertidriveGuida(rs.getInt("IDBlackBox"), rs.getLong("IDZBRecZ"),
                            rs.getInt("CertidriveIDConducente"),
                            rs.getInt("IDGuida"), rs.getTimestamp("BTimeStamp"), rs.getInt("FuelLevel"));
                }
                // aggiorno lo stato del record
                if (CertidriveIDGuida != -1) {
                    QueryString = "UPDATE ZBRecZ SET Stato=2,CertidriveIDGuida=? WHERE IDZBRecZ=?";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setInt(1, CertidriveIDGuida);    //CertidriveIDGuida
                    statement1.setInt(2, rs.getInt("IDZBRecZ"));    //IDRec
                    statement1.execute();
                    statement1.close();
                } else {
                    //in caso di errore si pone la condizione di errore nello stato
                    QueryString = "UPDATE ZBRecZ SET Stato=3,CertidriveIDGuida=? WHERE IDZBRecZ=?";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setInt(1, CertidriveIDGuida);    //CertidriveIDGuida
                    statement1.setInt(2, rs.getInt("IDZBRecZ"));    //IDRec
                    statement1.execute();
                    statement1.close();
                    Log.WriteLog(3, "Errore di registrazione del record ID " + rs.getInt(4) + " in CertidriveWeb");
                }
            }

            // tratto tutti i record y in coda


        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    public int CertidriveCheckRecordsZXY_old() {
        PreparedStatement statement, statement1;
        ResultSet rs;

        CheckConnection();
        try {
            if (DbAdminConn.isClosed()) {
                return -1;
            }
        } catch (SQLException ex) {
            return -1;
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException ex) {
            Logger.getLogger(ZB_ElaboraDati.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            // esegue la query al DB
            String QueryString = "SELECT RZ.IDZBRecZ,RZ.IDBlackBox,RZ.stato,RZ.CertidriveIDGuida, RZ.IDGuida,"
                    + " RZ.BTimeStamp, RZ.FuelLevel, BB.IDAzienda, AA.CertidriveIDAzienda, TK.CertidriveIDConducente"
                    + " FROM zbrecz_old RZ"
                    + " LEFT JOIN BlackBox BB ON   RZ.IDBlackBox= BB.IDBlackBox"
                    + " LEFT JOIN Azienda AA ON BB.IDAzienda=AA.IDAzienda"
                    + " LEFT JOIN Token TK ON TK.IDToken=RZ.IDToken"
                    + " WHERE RZ.stato=0 and AA.CertidriveIDAzienda is not null"
                    + " and AA.IDAzienda=15"
                    + " order by RZ.IDBlackBox,RZ.IDZBRecZ;";

            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();
            while (rs.next()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ex) {
                    Logger.getLogger(ZB_ElaboraDati.class.getName()).log(Level.SEVERE, null, ex);
                }
                int CertidriveIDGuida = -1;
                if (rs.getInt("CertidriveIDGuida") > 0) {
                    // esegue Update del record Z nel DB di Certidrive
                    CertidriveIDGuida = rs.getInt("CertidriveIDGuida");
                    try {
                        int ret = UpdateCertidriveGuida_old(rs.getInt("IDBlackBox"), rs.getLong("IDZBRecZ"),
                                rs.getInt("CertidriveIDConducente"),
                                rs.getInt("IDGuida"), CertidriveIDGuida,
                                rs.getTimestamp("BTimeStamp"), rs.getInt("FuelLevel"));
                        if (ret == 0) {
                            CertidriveIDGuida = -1;
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    // esegue insert di un nuovo record di guida nel DB di Certidrive
                    CertidriveIDGuida = InsertCertidriveGuida(rs.getInt("IDBlackBox"), rs.getLong("IDZBRecZ"),
                            rs.getInt("CertidriveIDConducente"),
                            rs.getInt("IDGuida"), rs.getTimestamp("BTimeStamp"), rs.getInt("FuelLevel"));
                }
                // aggiorno lo stato del record
                if (CertidriveIDGuida != -1) {
                    QueryString = "UPDATE ZBRecZ_old SET Stato=2,CertidriveIDGuida=? WHERE IDZBRecZ=?";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setInt(1, CertidriveIDGuida);    //CertidriveIDGuida
                    statement1.setInt(2, rs.getInt("IDZBRecZ"));    //IDRec
                    statement1.execute();
                    statement1.close();
                } else {
                    //in caso di errore si pone la condizione di errore nello stato
                    QueryString = "UPDATE ZBRecZ_old SET Stato=3,CertidriveIDGuida=? WHERE IDZBRecZ=?";
                    statement1 = DbAdminConn.prepareStatement(QueryString);
                    statement1.setInt(1, CertidriveIDGuida);    //CertidriveIDGuida
                    statement1.setInt(2, rs.getInt("IDZBRecZ"));    //IDRec
                    statement1.execute();
                    statement1.close();
                    Log.WriteLog(3, "Errore di registrazione del record ID " + rs.getInt(4) + " in CertidriveWeb");
                }
            }

            // tratto tutti i record y in coda
//            DbAdminConn.commit();

        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    /**
     * aggiorna la lista dei comandi di abilitazione alla guida per la ZB
     * indicata
     *
     * @param ZB da aggiornare
     * @return true se aggiornamento lanciato con successo false altrimenti
     */
    public boolean UpdateZBDriver(zbox ZB) {
        PreparedStatement statement, statement1;
        ResultSet rs, rs1;

        CheckConnection();

        try {
            if (DbAdminConn.isClosed()) {
                return false;
            }
        } catch (SQLException ex) {
            return false;
        }
        try {
            String CMD = "USB-Rst:";
            // esegue la query al DB
            String QueryString = "SELECT IDZBCommand, IDBlackBox, Command, Stato, Reply FROM ZBCommand \n\r"
                    + " WHERE IDBlackBox=" + ZB.IDBlackBox + " AND Command=? AND Stato=0"
                    + " ORDER BY IDZBCommand";
            statement = DbAdminConn.prepareStatement(QueryString);
            statement.setString(1, CMD);
            rs = statement.executeQuery();
            if (rs.next()) {
                CMD = "USB-Enb:%";
                QueryString = "UPDATE  ZBCommand SET Stato=2 \n\r"
                        + " WHERE IDBlackBox=" + ZB.IDBlackBox + " AND Command LIKE ? AND Stato=0";
                statement1 = DbAdminConn.prepareStatement(QueryString);
                statement1.setString(1, CMD);
                statement1.execute();
                ArrayList<Token> Tokens = GetTokens(ZB);
                if (Tokens != null) {
                    for (int i = 0; i < Tokens.size(); i++) {
                        CMD = "USB-Enb:" + Tokens.get(i).GetTokenHexString() + ";" + Tokens.get(i).GetTokenHexString() + ";20201231_125900";
                        String QueryString1 = "SELECT IDZBCommand, IDBlackBox, Command, Stato, Reply FROM ZBCommand \n\r"
                                + " WHERE IDBlackBox=" + ZB.IDBlackBox + " AND Command= ? AND Stato=2 AND IDZBCommand>?"
                                + " ORDER BY IDZBCommand";
                        statement = DbAdminConn.prepareStatement(QueryString1);
                        statement.setString(1, CMD);
                        statement.setInt(2, rs.getInt("IDZBCommand"));
                        rs1 = statement.executeQuery();
                        if (rs1.next()) {
                            QueryString = "UPDATE  ZBCommand SET Stato=0 \n\r"
                                    + " WHERE IDZBCommand= ? AND Stato=2";
                            statement1 = DbAdminConn.prepareStatement(QueryString);
                            statement1.setInt(1, rs1.getInt("IDZBCommand"));
                            statement1.execute();
                        } else {
                            InsertCommand(ZB, CMD);
                        }
                        rs1.close();
                    }
                }
                CMD = "USB-Enb:%";
                QueryString = "UPDATE  ZBCommand SET Stato=3 \n\r"
                        + " WHERE IDBlackBox=" + ZB.IDBlackBox + " AND Command LIKE ? AND Stato=2";
                statement1 = DbAdminConn.prepareStatement(QueryString);
                statement1.setString(1, CMD);
                statement1.execute();
                statement1.close();
            } else {
                InsertCommand(ZB, "USB-Rst:");
                ArrayList<Token> Tokens = GetTokens(ZB);
                if (Tokens != null) {
                    for (int i = 0; i < Tokens.size(); i++) {
                        CMD = "USB-Enb:" + Tokens.get(i).GetTokenHexString() + ";" + Tokens.get(i).GetTokenHexString() + ";20201231_125900";
                        InsertCommand(ZB, CMD);
                    }
                }
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;

    }
    // gestione Token

    public Token GetToken(int IDToken) {
        PreparedStatement statement;
        ResultSet rs;
        Token TK = null;

        CheckConnection();

        try {
            if (DbAdminConn.isClosed()) {
                return null;
            }
        } catch (SQLException ex) {
            return null;
        }
        try {
            // esegue la query al DB
            String QueryString = "SELECT * from Token \n\r"
                    + " WHERE IDToken=" + IDToken;
            statement = DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();

            if (rs.next()) {
                TK = new Token();
                TK.IDToken = rs.getInt("IDToken");
                TK.Cognome = rs.getString("Cognome");
                TK.Nome = rs.getString("Nome");
                TK.CDIDconducente = rs.getInt("CertidriveIDConducente");
                TK.IDAzienda = rs.getInt("IDAzienda");
                TK.SetTokenHexString(rs.getString("TokenHWID"));
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return TK;
    }

    /**
     * ritorna la lista dei Token Associati alla ZBox ZB
     *
     * @param ZB ZBox da verificare
     * @return
     */
    public ArrayList<Token> GetTokens(zbox ZB) {
        PreparedStatement statement;
        ResultSet rs;
        ArrayList<Token> A;

        CheckConnection();

        try {
            if (DbAdminConn.isClosed()) {
                return null;
            }
        } catch (SQLException ex) {
            return null;
        }
        try {
            // esegue la query al DB
            String QueryString = "SELECT ZBD.IDZBDriver, ZBD.IDBlackBox, ZBD.IDToken,"
                    + "TK.Cognome, TK.Nome, TK.CertidriveIDConducente, TK.TokenHWID FROM ZBDriver ZBD \n\r"
                    + " LEFT JOIN token TK ON ZBD.IDToken=TK.IDToken \n\r"
                    + " WHERE ZBD.IDBlackBox=" + ZB.IDBlackBox
                    + " ORDER BY TK.TokenHWID";
            statement = DbAdminConn.prepareStatement(QueryString);
//            statement.setInt(1, ZB.IDBlackBox);
            rs = statement.executeQuery(QueryString);
            A = new ArrayList<Token>();
            while (rs.next()) {
                Token TK = new Token();
                TK.IDToken = rs.getInt("IDToken");
                TK.Cognome = rs.getString("Cognome");
                TK.Nome = rs.getString("Nome");
                TK.CDIDconducente = rs.getInt("CertidriveIDConducente");
                TK.SetTokenHexString(rs.getString("TokenHWID"));
                A.add(TK);
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return A;
    }

    public ArrayList<Token> GetTokensAzienda(Azienda Az) {
        PreparedStatement statement;
        ResultSet rs;
        ArrayList<Token> A;

        try {
            if (DbAdminConn.isClosed()) {
                return null;
            }
        } catch (SQLException ex) {
            return null;
        }
        try {
            // esegue la query al DB
            String QueryString = "SELECT * from Token"
                    + " WHERE IDAzienda=" + Az.IDAzienda
                    + " ORDER BY TokenHWID";
            statement = DbAdminConn.prepareStatement(QueryString);
//            statement.setInt(1, ZB.IDBlackBox);
            rs = statement.executeQuery(QueryString);
            A = new ArrayList<Token>();
            while (rs.next()) {
                Token TK = new Token();
                TK.IDToken = rs.getInt("IDToken");
                TK.Cognome = rs.getString("Cognome");
                TK.Nome = rs.getString("Nome");
                TK.CDIDconducente = rs.getInt("CertidriveIDConducente");
                TK.SetTokenHexString(rs.getString("TokenHWID"));
                A.add(TK);
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return A;
    }

    public boolean AddToken(zbox ZB, Token TK) {
        try {
            if (TK == null || ZB == null) {
                return false;
            }
            if (TK.IDToken == -1 || ZB.IDBlackBox == -1) {
                return false;
            }

            PreparedStatement statement;

            String QueryString = "INSERT INTO ZBDriver ( IDBlackBox, IDToken ) VALUES ( ?, ? )"; //+
            statement = DbAdminConn.prepareStatement(QueryString);

            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, TK.IDToken);

            Log.WriteLog(4, QueryString);

            try {
                int res = statement.executeUpdate();
                if (res == 0) {
                    Log.WriteLog(1, "Errore di inserimento dell TOKEN nel DB");
                }
            } catch (Exception ex) {
                Log.WriteEx(DBAdminClass.class.getName(), ex);
                Log.WriteLog(1, "Errore di inserimento dell TOKEN nel DB");
                statement.close();
                return false;
            }

            return true;
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public boolean DelToken(zbox ZB, Token TK) {
        try {
            if (TK == null || ZB == null) {
                return false;
            }
            if (TK.IDToken == -1 || ZB.IDBlackBox == -1) {
                return false;
            }

            PreparedStatement statement;

            String QueryString = "DELETE FROM ZBDriver  WHERE (IDBlackBox=? AND IDToken=? )";
            statement = DbAdminConn.prepareStatement(QueryString);

            statement.setInt(1, ZB.IDBlackBox);
            statement.setLong(2, TK.IDToken);

            Log.WriteLog(4, QueryString);

            try {
                int res = statement.executeUpdate();
                if (res == 0) {
                    Log.WriteLog(1, "Errore di Rimozione dell TOKEN nel DB");
                }
            } catch (Exception ex) {
                Log.WriteEx(DBAdminClass.class.getName(), ex);
                Log.WriteLog(1, "Errore di Rimozione dell TOKEN nel DB");
                statement.close();
                return false;
            }

            return true;
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    //da rimuovere
    public boolean InsertCommand(zbox ZB, String Command) {
        try {
            PreparedStatement statement;

            String QueryString = "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato ) VALUES ( ?, ?, ? )"; //+
            statement = DbAdminConn.prepareStatement(QueryString);

            statement.setInt(1, ZB.IDBlackBox);
            statement.setString(2, Command);
            statement.setInt(3, 0);

            Log.WriteLog(4, "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato ) VALUES ( " + ZB.IDBlackBox + ", '" + Command + "', 0 )");

//            Log.WriteLog(4,QueryString);

            try {
//                int res=0;
                int res = statement.executeUpdate();
                if (res == 0) {
                    Log.WriteLog(1, "Errore di inserimento dell COMANDO nel DB");
                }
            } catch (Exception ex) {
                Log.WriteEx(DBAdminClass.class.getName(), ex);
                Log.WriteLog(1, "Errore di inserimento dell COMANDO nel DB");
                statement.close();
                return false;
            }

            return true;
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    public boolean InsertCommand(Connection conn, zbox ZB, String Command, String ReqID, Timestamp timeout) throws SQLException {
        PreparedStatement  statement;
        String QueryString = "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato, ReqID, Timeout, Time) VALUES ( ?, ?, ? ,?, ?,?)"; //+
        statement = conn.prepareStatement(QueryString);
        try {
            statement.setInt(1, ZB.IDBlackBox);
            statement.setString(2, Command);
            statement.setInt(3, 0);
            statement.setString(4, ReqID);
            statement.setTimestamp(5, timeout);
            statement.setTimestamp(6, (new java.sql.Timestamp(System.currentTimeMillis())));
            
            int res = statement.executeUpdate();
            if (res==0) {
                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE,"Errore di inserimento del COMANDO nel DB");
                return false;
            }
        }finally {
            statement.close();
        }
        return false;
    }

    public boolean SetPushNotifyStatus(int IDBlackBox, int Stato) {
        try {
            PreparedStatement statement;
            ResultSet rs;
            String QueryString = "SELECT IDZBPushNotify,IDBlackBox,Stato from  ZBPushNotify"
                    + " WHERE IDBlackBox=?";
            statement = DbAdminConn.prepareStatement(QueryString);

            statement.setInt(1, IDBlackBox);


            Log.WriteLog(4, QueryString);

            rs = statement.executeQuery();
            Timestamp Now = new Timestamp((new java.util.Date()).getTime());
            if (rs.next()) {
                int IDZBPushNotify = rs.getInt("IDZBPushNotify");
                rs.close();
                statement.close();

                QueryString = "UPDATE ZBPushNotify SET Stato=?, LastSent=? WHERE IDZBPushNotify=?"; //+
                statement = DbAdminConn.prepareStatement(QueryString);

                statement.setInt(1, Stato);
                statement.setTimestamp(2, Now);
                statement.setInt(3, IDZBPushNotify);

                Log.WriteLog(4, QueryString);
            } else {
                QueryString = "INSERT INTO ZBPushNotify ( IDBlackBox, Stato, LastSent ) VALUES ( ?, ?, ? )"; //+
                statement = DbAdminConn.prepareStatement(QueryString);

                statement.setInt(1, IDBlackBox);
                statement.setInt(2, Stato);
                statement.setTimestamp(3, Now);

                Log.WriteLog(4, QueryString);

            }

            try {
                int res = statement.executeUpdate();
                if (res == 0) {
                    Log.WriteLog(1, "Errore di inserimento del Push Notify nel DB");
                }
            } catch (Exception ex) {
                Log.WriteEx(DBAdminClass.class.getName(), ex);
                Log.WriteLog(1, "Errore di inserimento del Push Notify nel DB");
                statement.close();
                return false;
            }

            return true;
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public static int uBToI(byte b) {
        return (int) b & 0xFF;
    }
}
