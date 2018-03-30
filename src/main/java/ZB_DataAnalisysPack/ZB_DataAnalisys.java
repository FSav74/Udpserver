/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ZB_DataAnalisysPack;

import com.viacom.DB.DBAdminClass;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import zb_udp_server.ConfClass;
import zb_udp_server.LogsClass;

/**
 *
 * @author Luca
 */
public class ZB_DataAnalisys {

    double r = 6372795.477598; // Raggio terrestre
    DBAdminClass DBAdmin;
    ConfClass Conf;
    LogsClass Log;
    ArrayList<String> message;

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin, ConfClass LConf, LogsClass LLog) {
        DBAdmin = LDBAdmin;
        Conf = LConf;
        Log = LLog;
        return true;
    }

    public int ZBDataRunCheck() {

        message = new ArrayList<String>();

        Statement statement;
        PreparedStatement statement1;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDconnbbtelecomflotta, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent "
                    + "from connbbtelecomflotta WHERE Active=1";
            System.out.println("ZB_DataAnalizer Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and BTimestamp>? and BTimestamp<?"
                        + " Order by BTimestamp";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setTimestamp(2, new Timestamp(now.getTime() - 36 * 60 * 60 * 1000));
                statement1.setTimestamp(3, new Timestamp(now.getTime() + 24 * 60 * 60 * 1000));

                ArrayList<ZB_RecordLoc> records = new ArrayList<ZB_RecordLoc>();

                rs1 = statement1.executeQuery();
                while (rs1.next()) {
                    ZB_RecordLoc Loc = new ZB_RecordLoc();
                    Loc.IDZBLocalization = rs1.getLong("IDZBLocalization");
                    Loc.IDBlackBox = rs1.getLong("IDBlackBox");
                    Loc.BLat = rs1.getFloat("BLat");
                    Loc.BLong = rs1.getFloat("BLong");
                    Loc.BTimestamp = rs1.getTimestamp("BTimeStamp");
                    Loc.StatoZB = rs1.getInt("StatoZB");
                    Loc.ValidGPS = rs1.getInt("ValidGPS");
                    Loc.QualityGPS = rs1.getInt("QualityGPS");

                    records.add(Loc);
                }
                rs1.close();
                statement1.close();

//                ControllaSalti(records);
//                ControllaMancatiFix(records);

//                ControllaLungheAssenzeComunicazione(IDBlackBox);
                ControllaComunicazioniTroppoVeloci(IDBlackBox);



            }
            rs.close();
            statement.close();

            System.out.println("ZB_DataAnlizer Stop");

            try {
                Thread.sleep(1 * 60 * 1000);        // attendo un 1minuto prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            DBAdmin.DbAdminConn.commit();
            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * verifica la presenza di salti ed eventualmente ritorna lo id della
     * localizzazione che ha presentato il salto
     *
     * @param records
     * @return
     */
    long ControllaSalti(ArrayList<ZB_RecordLoc> records) {
        float PrevLat, PrevLong;
        Timestamp PrevTimestamp;
        int PrevStatoZB;

        if (records.size() < 2) {
            return -1;
        }
        PrevLat = records.get(0).BLat;
        PrevLong = records.get(0).BLong;
        PrevTimestamp = records.get(0).BTimestamp;
        PrevStatoZB = records.get(0).StatoZB;


        for (int i = 1; i < records.size(); i++) {

            double radLatA = Math.PI * records.get(i).BLat / 180.0;
            double radLonA = Math.PI * records.get(i).BLong / 180.0;
            double radLatB = Math.PI * PrevLat / 180.0;
            double radLonB = Math.PI * PrevLong / 180.0;
            double phi = Math.abs(radLonA - radLonB);
            double dist = r * Math.acos((Math.sin(radLatA) * Math.sin(radLatB)) + (Math.cos(radLatA) * Math.cos(radLatB) * Math.cos(phi)));

            long TimeDiff_sec = (records.get(i).BTimestamp.getTime() - PrevTimestamp.getTime()) / 1000;
            double Speed = (dist / TimeDiff_sec) * 3.6; // in KM/h


            if ((dist > 5000 && PrevStatoZB == 1 && records.get(0).StatoZB == 1)
                    || (dist > 15000 && PrevStatoZB == 3 && records.get(0).StatoZB == 3)
                    || Speed > 140) {
                String msg = "Salto per ZB ID=" + records.get(i).IDBlackBox + " Loc=" + records.get(i).IDZBLocalization
                        + " TS=" + records.get(i).BTimestamp + " Stato=" + records.get(i).StatoZB
                        + "   ---   Dist=" + String.format("%10.2f", dist) + " Tempo=" + String.format("%5d", TimeDiff_sec)
                        + "   speed=" + String.format("%8.2f", Speed) + " km/h";

                System.out.println(msg);
                message.add(msg);
            }
            PrevLat = records.get(i).BLat;
            PrevLong = records.get(i).BLong;
            PrevTimestamp = records.get(i).BTimestamp;
            PrevStatoZB = records.get(i).StatoZB;
        }
        return -1;
    }

    /**
     * verfica la presenza che durante il periodo di guida non ci siano piu' di
     * 10 fix consecutivi non validi
     *
     * @param records
     * @return
     */
    long ControllaMancatiFix(ArrayList<ZB_RecordLoc> records) {
        float PrevLat, PrevLong;
        Timestamp PrevTimestamp;
        int PrevStatoZB;

        if (records.size() < 2) {
            return -1;
        }
        PrevLat = records.get(0).BLat;
        PrevLong = records.get(0).BLong;
        PrevTimestamp = records.get(0).BTimestamp;
        PrevStatoZB = records.get(0).StatoZB;
        int StatoGuida;
        if (records.get(0).StatoZB > 1) {
            StatoGuida = 1;
        } else {
            StatoGuida = 0;
        }

        int ConsecutiveFailFix = 0;


        for (int i = 1; i < records.size(); i++) {
            if (StatoGuida == 1 && records.get(i).StatoZB > 1) {
                if (records.get(i).ValidGPS == 1) {
                    ConsecutiveFailFix = 0;
                } else {
                    ConsecutiveFailFix++;
                }
            } else if (StatoGuida == 0) {
                ConsecutiveFailFix = 0;
            }

            if (ConsecutiveFailFix == 10) {
                String msg = "Mancanza Fix in guida per ZB ID=" + records.get(i).IDBlackBox + " Loc=" + records.get(i).IDZBLocalization
                        + " TS=" + records.get(i).BTimestamp + " Stato=" + records.get(i).StatoZB;
                System.out.println(msg);
                message.add(msg);
                ConsecutiveFailFix = 0;
            }
        }

        return -1;
    }

    /**
     * verfica la presenza che durante il periodo di guida non ci siano piu' di
     * 10 fix consecutivi non validi
     *
     * @param records
     * @return
     */
    long ControllaLungheAssenzeComunicazione(long IDBlackBox) {
        PreparedStatement statement1, statement2;
        long IDZBLocalization = 0;
        Timestamp BTimestamp = null, LastContact = null;
        String BBSerial = "", Targa = "";
        int StatoZB = -1;
        ResultSet rs1, rs2;

        Timestamp now = new Timestamp((new java.util.Date()).getTime());
        try {
            String QueryString = "SELECT L.IDZBLocalization,L.IDBlackBox,L.BLat,L.BLong,L.BTimestamp,B.LastContact,L.StatoZB,L.ValidGPS,L.QualityGPS,"
                    + " B.BBSerial, B.Targa"
                    + " FROM zblocalization L"
                    + " Left join BlackBox B on B.IDBlackBox=L.IDBlackBox"
                    + " where IDZBLocalization = (SELECT max(IDZBLocalization) id from zblocalization "
                    + "where IDBlackBox=?)";

            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

            statement1.setLong(1, IDBlackBox);

            rs1 = statement1.executeQuery();
            if (rs1.next()) {
                IDZBLocalization = rs1.getLong("IDZBLocalization");
                BTimestamp = rs1.getTimestamp("BTimestamp");
                StatoZB = rs1.getInt("StatoZB");
                LastContact = rs1.getTimestamp("LastContact");
                BBSerial = rs1.getString("BBSerial");
                Targa = rs1.getString("Targa");
            } else {
                QueryString = "SELECT L.IDZBLocalization,L.IDBlackBox,L.BLat,L.BLong,L.BTimestamp,B.LastContact,L.StatoZB,L.ValidGPS,L.QualityGPS,"
                        + " B.BBSerial, B.Targa"
                        + " FROM zblocalization_old L"
                        + " Left join BlackBox B on B.IDBlackBox=L.IDBlackBox"
                        + " where IDZBLocalization = (SELECT max(IDZBLocalization) id from zblocalization_old "
                        + "where IDBlackBox=?)";

                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement2.setLong(1, IDBlackBox);

                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    IDZBLocalization = rs2.getLong("IDZBLocalization");
                    BTimestamp = rs2.getTimestamp("BTimestamp");
                    StatoZB = rs2.getInt("StatoZB");
                    LastContact = rs2.getTimestamp("LastContact");
                    BBSerial = rs2.getString("BBSerial");
                    Targa = rs2.getString("Targa");
                }
                rs2.close();
                statement2.close();


            }
            rs1.close();
            statement1.close();

            if (IDZBLocalization != 0 && BTimestamp != null) {
                if (now.getTime() > BTimestamp.getTime() + (20 * 24 * 60 * 60 * 1000)) {
                    String msg = "ZB ID=" + IDBlackBox + " SN=" + BBSerial + " Targa=" + Targa + " non comunicante TS=" + BTimestamp + " LastContact=" + LastContact + " Stato=" + StatoZB;
                    System.out.println(msg);
                    message.add(msg);
                }
            }

        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
        return 0;
    }

    /**
     * verfica la presenza che durante il periodo di guida non ci siano piu' di
     * 10 fix consecutivi non validi
     *
     * @param records
     * @return
     */
    long ControllaComunicazioniTroppoVeloci(long IDBlackBox) {
        PreparedStatement statement1, statement2;
        long IDZBLocalization = 0;
        Timestamp BTimestamp = null, BTimestampPre = null, LastContact = null;
        String BBSerial = "", Targa = "";
        int StatoZB = -1, StatoZBPre = -1;
        ResultSet rs1, rs2;
        long AvgTime = 0;
        int AvgCount = 0;

        Timestamp now = new Timestamp((new java.util.Date()).getTime());
        try {

            String QueryString = "SELECT L.BTimestamp,L.StatoZB,B.BBSerial, B.Targa"
                    + " FROM zblocalization L"
                    + " Left join BlackBox B on B.IDBlackBox=L.IDBlackBox"
                    + " where L.BTimestamp > ? and L.IDBlackBox=? "
                    + "order by L.BTimestamp";

            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            DBAdmin.DbAdminConn.setAutoCommit(true);


            Timestamp yesterday = new Timestamp((new java.util.Date()).getTime() - 24 * 60 * 60 * 1000);
            statement1.setTimestamp(1, yesterday);
            statement1.setLong(2, IDBlackBox);

            rs1 = statement1.executeQuery();

            if (rs1.next()) {
//                IDZBLocalization=rs1.getLong("IDZBLocalization");
                BTimestampPre = rs1.getTimestamp("BTimestamp");
                StatoZBPre = rs1.getInt("StatoZB");
//                LastContact=rs1.getTimestamp("LastContact");
                BBSerial = rs1.getString("BBSerial");
                Targa = rs1.getString("Targa");

                while (rs1.next()) {
                    BTimestamp = rs1.getTimestamp("BTimestamp");
                    StatoZB = rs1.getInt("StatoZB");
                    if (StatoZBPre > 1 && StatoZB > 1 && BTimestamp.getTime() != BTimestampPre.getTime()) {
                        long TimeDiff = BTimestamp.getTime() - BTimestampPre.getTime();
                        AvgTime += TimeDiff;
                        AvgCount++;
                    }
                    BTimestampPre = BTimestamp;
                    StatoZBPre = StatoZB;
                }
                if (AvgCount > 0 && AvgTime > 0) {
                    float Avg = AvgTime / AvgCount;
                    if (Avg < 100 * 1000) {
                        String msg = "ZB ID=" + IDBlackBox + " SN=" + BBSerial + " Targa=" + Targa + " ritardo medio di " + Avg;
                        System.out.println(msg);
                        message.add(msg);

                        /*                        String QueryString1 = "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato ) VALUES ( ?, ?, ? )"; //+
                         statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString1);

                         statement2.setLong(1, IDBlackBox);
                         statement2.setString(2, "GPRT.RLTR=;151");
                         statement2.setInt(3, 0);
                        
                         statement2.executeUpdate();
                        
                         statement2.close();
                         QueryString1 = "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato ) VALUES ( ?, ?, ? );\n\r"; //+
                         statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString1);

                         statement2.setLong(1, IDBlackBox);
                         statement2.setString(2, "GENR.TTDS=;86400");
                         statement2.setInt(3, 0);
                         statement2.executeUpdate();
                        
                         statement2.close();*/
                        System.out.println("Lanciato comando di correzione");


                    }

                }
            }
            rs1.close();
            statement1.close();


        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
        return 0;
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti() {

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1, rs2;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDconnbbtelecomflotta, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from connbbtelecomflotta WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE connbbtelecomflotta SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti Stop");

            RipristinaSalti_NewDest();

            try {
                Thread.sleep(1 * 60 * 1000);        // attendo un 1minuto prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_NewDest() {

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1, rs2;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDconnbbtelecomflotta, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from connbbtelecomflotta_newdest WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti_NewDest Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE connbbtelecomflotta_newdest SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti_NewDest Stop");

            try {
                Thread.sleep(1 * 60 * 1000);        // attendo un 1minuto prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }

    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_connbbgruppoerg() {

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDConnbbGruppoErg, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from connbbgruppoerg WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE connbbgruppoerg SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti connbbgruppoerg Stop");

            try {
                Thread.sleep(1 * 1000);        // attendo un 1 secondo prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_connbbALD() {

        String ZB_DB_Table = "connbbALD";

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from " + ZB_DB_Table + " WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti " + ZB_DB_Table + " Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE " + ZB_DB_Table + " SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " " + ZB_DB_Table + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " " + ZB_DB_Table + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti " + ZB_DB_Table + " Stop");

            try {
                Thread.sleep(1 * 1000);        // attendo un 1 secondo prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_connbb(String ZB_DB_Table) {

//        String ZB_DB_Table="connbbLML";

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from " + ZB_DB_Table + " WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti " + ZB_DB_Table + " Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE " + ZB_DB_Table + " SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " " + ZB_DB_Table + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " " + ZB_DB_Table + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti " + ZB_DB_Table + " Stop");

            try {
                Thread.sleep(1 * 100);        // attendo un 1 secondo prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_connbbCONAP_production() {

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDConnbbConap, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from connbbconap_production WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti connbbconap_production Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE connbbconap_production SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti connbbconap_production Stop");

            try {
                Thread.sleep(1 * 1000);        // attendo un 1 secondo prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_connbbCONAP() {

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDConnbbConap, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from connbbconap WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti connbbconap Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE connbbconap SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti connbbconap Stop");

            try {
                Thread.sleep(1 * 1000);        // attendo un 1 secondo prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_connbbCOAP_production() {

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDConnbbCoap, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from connbbcoap_production WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti connbbcoap_production Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE connbbcoap_production SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti connbbcoap_production Stop");

            try {
                Thread.sleep(1 * 1000);        // attendo un 1 secondo prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Ripristina i veicoli che sono stati bloccati per salto GPS
     *
     * @param records
     * @return
     */
    public long RipristinaSalti_connbbCOAP() {

        Statement statement;
        PreparedStatement statement1, statement2;
        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }
        try {
            DBAdmin.DbAdminConn.setAutoCommit(false);

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "select IDConnbbCoap, IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, temporaryDisabled "
                    + "from connbbcoap WHERE Active=1 and temporaryDisabled=1";
            System.out.println("RipristinaSalti connbbcoap Start");

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                long IDBlackBox = rs.getLong("IDBlackBox");
                QueryString = "SELECT IDZBLocalization,IDBlackBox,BLat,BLong,BTimestamp,StatoZB,ValidGPS,QualityGPS"
                        + " FROM zblocalization "
                        + " WHERE IDBlackBox=? and IDZBLocalization>? and ValidGPS=1"
                        + " ORDER BY IDZBLocalization";

                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                statement1.setLong(1, IDBlackBox);
                statement1.setLong(2, rs.getLong("LastLocationSent"));

                rs1 = statement1.executeQuery();
                if (rs1.next()) {
                    QueryString = "UPDATE connbbcoap SET LastLocationSent=?, temporaryDisabled=0"
                            + " WHERE IDBlackBox=?";

                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

                    statement2.setLong(1, rs1.getLong("IDZBLocalization"));
                    statement2.setLong(2, IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    System.out.println("IDBB=" + IDBlackBox + " Risolto Salto");
                } else {
                    System.out.println("IDBB=" + IDBlackBox + " Nessuna Localizzazione Trovata");
                }
                rs1.close();
                statement1.close();


            }
            rs.close();
            statement.close();
            DBAdmin.DbAdminConn.commit();

            System.out.println("RipristinaSalti connbbcoap Stop");

            try {
                Thread.sleep(1 * 1000);        // attendo un 1 secondo prima di ricominciare
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_DataAnalisys.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }
}
