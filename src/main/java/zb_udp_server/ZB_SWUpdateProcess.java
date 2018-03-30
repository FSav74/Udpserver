/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class ZB_SWUpdateProcess {

    DBAdminClass DBAdmin;
    ConfClass Conf;
    LogsClass Log;

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin, ConfClass LConf, LogsClass LLog) {
        DBAdmin = LDBAdmin;
        Conf = LConf;
        Log = LLog;
        return true;
    }

    /*    @Override
     @SuppressWarnings("SleepWhileInLoop")
     public void run() {
        
     if (Conf.SWUpdateProcess==0) {
     Log.WriteLog(1, "Elaboratore dei SW update NON attivato");
     }
     Log.WriteLog(1, "Avvio dell' Elaboratore dei record");
     DBAdmin = new DBAdminClass();
     DBAdmin.SetConf(Conf);
     DBAdmin.SetLog(Log);
     while (!DBAdmin.Init()) {
     try {
     Thread.sleep(30000);
     } catch (InterruptedException ex) {
     Logger.getLogger(ZB_ElaboraDati.class.getName()).log(Level.SEVERE, null, ex);
     }
     }
        
     while(true) {
            
     }
     }*/
    public int SWUpdateStatusCheck() {

        Statement statement, statement1;
        int RunningTransfer = 1000;
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

            String QueryString = "SELECT SW.IDSWUpProcess, SW.IDBlackBox, SW.Stato, SW.LastCheck, SW.IDCommandDownload, SW.IDZBFileDownload, "
                    + " SW.IDCommandSWUP, SW.IDZBFileDownloadSession, SW.LastSentChunk_Date, SW.LastCommandSent_Date, Dw.FileName, "
                    + " SW.CommandPre, SW.IDCommandPre, SW.CommandPost, SW.IDCommandPost"
                    + " FROM swupprocess SW "
                    + " LEFT JOIN ZBFileDownload Dw on Dw.IDZBFileDownload=SW.IDZBFileDownload "
                    + " WHERE Stato<20 ";
//                    + " WHERE Stato<20 and IDBlackBox=9466";

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                String QueryString1 = "SELECT count(*) C FROM swupprocess SW WHERE SW.Stato=1 or SW.Stato=2 or SW.Stato=5";

                statement1 = DBAdmin.DbAdminConn.createStatement();

                rs1 = statement1.executeQuery(QueryString1);
                if (rs1.next()) {
                    RunningTransfer = rs1.getInt("C");
                }
                rs1.close();
                statement1.close();

                ZB_SWUpdate_Record SWUp_Rec = new ZB_SWUpdate_Record();
                SWUp_Rec.IDSWUpProcess = rs.getLong("IDSWUpProcess");
                SWUp_Rec.IDBlackBox = rs.getLong("IDBlackBox");
                SWUp_Rec.Stato = rs.getInt("Stato");
                SWUp_Rec.LastCheck = rs.getTimestamp("LastCheck");
                SWUp_Rec.IDCommandDownload = rs.getLong("IDCommandDownload");
                SWUp_Rec.IDCommandSWUP = rs.getLong("IDCommandSWUP");
                SWUp_Rec.IDZBFileDownload = rs.getLong("IDZBFileDownload");
                SWUp_Rec.IDZBFileDownloadSession = rs.getLong("IDZBFileDownloadSession");
                SWUp_Rec.LastSentChunk_Date = rs.getTimestamp("LastSentChunk_Date");
                SWUp_Rec.LastCommandSent_Date = rs.getTimestamp("LastCommandSent_Date");
                SWUp_Rec.FileName = rs.getString("FileName");
                SWUp_Rec.IDCommandPre = rs.getLong("IDCommandPre");
                SWUp_Rec.CommandPre = rs.getString("CommandPre");
                SWUp_Rec.CommandPost = rs.getString("CommandPost");
                SWUp_Rec.IDCommandPost = rs.getLong("IDCommandPost");

                Timestamp now = new Timestamp((new java.util.Date()).getTime());
                if (SWUp_Rec.LastCheck == null) {
                    SWUp_Rec.LastCheck = new Timestamp(now.getTime() - 20 * 60 * 60 * 1000);
                }
//                if (SWUp_Rec.LastSentChunk_Date==null) { SWUp_Rec.LastSentChunk_Date=new Timestamp(now.getTime()-20*60*60*1000);   }

                if (SWUp_Rec.Stato == 0) {
                    if (RunningTransfer < Conf.MaxUpdateSession) {
                        CheckStato0(SWUp_Rec);              // invio del comando di FileDownload
                    }
                } else if (SWUp_Rec.Stato == 1) {
                    CheckStato1(SWUp_Rec);              // attesa della acquisizione del comando di donwload
                } else if (SWUp_Rec.Stato == 2) {
                    CheckStato2(SWUp_Rec);              // attesa della fine del Download
                } else if (SWUp_Rec.Stato == 3) {
                    CheckStato3(SWUp_Rec);              //invio del comando di SWUpdate
                } else if (SWUp_Rec.Stato == 4) {
                    CheckStato4(SWUp_Rec);              // attesa del messaggio di INFO
                } else if (SWUp_Rec.Stato == 5) {
                    CheckStato5(SWUp_Rec);              // attesa delll'acquisizione del messaggio di pre configurazione
                } else if (SWUp_Rec.Stato == 6) {
                    CheckStato6(SWUp_Rec);              // attesa delll'acquisizione del messaggio di post configurazione
                }
                DBAdmin.DbAdminConn.commit();
            }
            rs.close();
            statement.close();

//            DBAdmin.DbAdminConn.commit();
            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    /**
     * Provvede a verificare che la ZBox sia in comunicazione e che non sia in
     * movimento; in questo momento prepara la sessione per il file download ed
     * aggiunge il comando.
     *
     * Se la ZBox è pronta per il download allora cambia lo stato della sessione
     * di SwUpdate ad 1 Altrimenti non modifica lo stato.
     *
     * @param SWUp_Rec: struttura dei dati di SWUpdate che vengono completati.
     * @return
     * @throws SQLException
     */
    int CheckStato0(ZB_SWUpdate_Record SWUp_Rec) throws SQLException {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if ((now.getTime() - SWUp_Rec.LastCheck.getTime()) > 20000) {  // esegue la verifica ogni 20 sec
            PreparedStatement statement, statement1, statement2, statement3;
            ResultSet rs;

            String QueryString = "SELECT * FROM zblocalization "
                    + " where IDBlackBox=? and StatoZB=1 "
                    + " and BTimeStamp>?";
            statement = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            statement.setLong(1, SWUp_Rec.IDBlackBox);
            statement.setTimestamp(2, new Timestamp(now.getTime() - 40 * 60 * 1000));
            rs = statement.executeQuery();

            if (rs.next()) {
                if (SWUp_Rec.IDCommandPre > 0 || SWUp_Rec.CommandPre != null) {
                    DoStato0_5(SWUp_Rec);
                } else {
                    DoStato0_1(SWUp_Rec);
//                    // identificazione del campo IDCommand
//                    ResultSet rs2;
//                    QueryString = "Select IDZBCommand FROM ZBCommand "
//                            + " where IDBlackBox=? and Stato =0 and Command LIKE 'GPR-Dwl:ON'"
//                            + " order by IDZBCommand Desc Limit 1";
//                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
//                    statement2.setLong(1, SWUp_Rec.IDBlackBox);
//                    rs2=statement2.executeQuery();
//                    if (rs2.next()) {
//                        SWUp_Rec.IDCommandDownload=rs2.getLong("IDZBCommand");                    
//                    } else {
//                        QueryString = "INSERT INTO ZBCommand (IDBlackBox, Command, Stato) VALUES(?,'GPR-Dwl:ON',0) ";
//                        statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
//                        statement3.setLong(1, SWUp_Rec.IDBlackBox);
//                        statement3.execute();
//                        ResultSet generatedKeys;
//                        generatedKeys = statement3.getGeneratedKeys();
//                        if (generatedKeys.next()) {
//                            SWUp_Rec.IDCommandDownload=generatedKeys.getLong(1);
//                            generatedKeys.close();
//                        }
//                        statement3.close();
//                    }
//                    rs2.close();
//                    statement2.close();
//
//                    // identificazione del campo IDFileDownloadSession
//                    QueryString = "Select IDZBFileDownloadSession FROM ZBFileDownloadSession "
//                            + " where IDBlackBox=? and IDZBFileDownload =? and Stato=0"
//                            + " order by IDZBFileDownloadSession Desc Limit 1";
//                    statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
//                    statement2.setLong(1, SWUp_Rec.IDBlackBox);
//                    statement2.setLong(2, SWUp_Rec.IDZBFileDownload);
//                    rs2=statement2.executeQuery();
//                    if (rs2.next()) {
//                        SWUp_Rec.IDZBFileDownloadSession=rs2.getLong("IDZBFileDownloadSession");                    
//                    } else {
//                        QueryString = "UPDATE ZBFileDownloadSession SET Stato=3 WHERE IDBlackBox=? AND Stato=0";
//                        statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
//                        statement3.setLong(1, SWUp_Rec.IDBlackBox);
//                        statement3.execute();
//                        statement3.close();
//                        QueryString = "INSERT INTO ZBFileDownloadSession (IDBlackBox, IDZBFileDownload, Stato, FilePathOnZBox)"
//                        + " VALUES(?,?,0,?) ";
//                        statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
//                        statement3.setLong(1, SWUp_Rec.IDBlackBox);
//                        statement3.setLong(2, SWUp_Rec.IDZBFileDownload);
//                        statement3.setString(3, "0:/SW/"+SWUp_Rec.FileName);
//                        statement3.execute();
//                        ResultSet generatedKeys;
//                        generatedKeys = statement3.getGeneratedKeys();
//                        if (generatedKeys.next()) {
//                            SWUp_Rec.IDZBFileDownloadSession=generatedKeys.getLong(1);
//                            generatedKeys.close();
//                        }
//                        statement3.close();
//                    }
//                    rs2.close();
//                    statement2.close();
//
//                    SWUp_Rec.LastCommandSent_Date=now;
//
//                    QueryString = "UPDATE swupprocess SET Stato=1,LastCheck=?,LastSentChunk_Date=null,IDCommandDownload=?"
//                            + ",IDCommandSWUP=null,IDZBFileDownloadSession=?,LastCommandSent_Date=? WHERE IDSWUpProcess=? ";
//                    statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
//                    statement1.setTimestamp(1, now);
//                    statement1.setLong(2, SWUp_Rec.IDCommandDownload);
//                    statement1.setLong(3, SWUp_Rec.IDZBFileDownloadSession);
//                    statement1.setTimestamp(4, SWUp_Rec.LastCommandSent_Date);
//                    statement1.setLong(5, SWUp_Rec.IDSWUpProcess);
//                    statement1.execute();
//                    statement1.close();
                }
                System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 0 IDCommandDownload=" + SWUp_Rec.IDCommandDownload + " IDZBFileDownloadSession=" + SWUp_Rec.IDZBFileDownloadSession);
            }
            statement.close();
            rs.close();
        }
        return 0;
    }

    int DoStato0_1(ZB_SWUpdate_Record SWUp_Rec) throws SQLException {
        PreparedStatement statement, statement1, statement2, statement3;
        ResultSet rs;
        Timestamp now = new Timestamp((new java.util.Date()).getTime());
        // identificazione del campo IDCommand
        ResultSet rs2;
        String QueryString = "Select IDZBCommand FROM ZBCommand "
                + " where IDBlackBox=? and Stato =0 and Command LIKE 'GPR-Dwl:ON'"
                + " order by IDZBCommand Desc Limit 1";
        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement2.setLong(1, SWUp_Rec.IDBlackBox);
        rs2 = statement2.executeQuery();
        if (rs2.next()) {
            SWUp_Rec.IDCommandDownload = rs2.getLong("IDZBCommand");
        } else {
            QueryString = "INSERT INTO ZBCommand (IDBlackBox, Command, Stato) VALUES(?,'GPR-Dwl:ON',0) ";
            statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement3.setLong(1, SWUp_Rec.IDBlackBox);
            statement3.execute();
            ResultSet generatedKeys;
            generatedKeys = statement3.getGeneratedKeys();
            if (generatedKeys.next()) {
                SWUp_Rec.IDCommandDownload = generatedKeys.getLong(1);
                generatedKeys.close();
            }
            statement3.close();
        }
        rs2.close();
        statement2.close();

        // identificazione del campo IDFileDownloadSession
        QueryString = "Select IDZBFileDownloadSession FROM ZBFileDownloadSession "
                + " where IDBlackBox=? and IDZBFileDownload =? and Stato=0"
                + " order by IDZBFileDownloadSession Desc Limit 1";
        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement2.setLong(1, SWUp_Rec.IDBlackBox);
        statement2.setLong(2, SWUp_Rec.IDZBFileDownload);
        rs2 = statement2.executeQuery();
        if (rs2.next()) {
            SWUp_Rec.IDZBFileDownloadSession = rs2.getLong("IDZBFileDownloadSession");
        } else {
            QueryString = "UPDATE ZBFileDownloadSession SET Stato=3 WHERE IDBlackBox=? AND Stato=0";
            statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            statement3.setLong(1, SWUp_Rec.IDBlackBox);
            statement3.execute();
            statement3.close();
            QueryString = "INSERT INTO ZBFileDownloadSession (IDBlackBox, IDZBFileDownload, Stato, FilePathOnZBox)"
                    + " VALUES(?,?,0,?) ";
            statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement3.setLong(1, SWUp_Rec.IDBlackBox);
            statement3.setLong(2, SWUp_Rec.IDZBFileDownload);
            statement3.setString(3, "0:/SW/" + SWUp_Rec.FileName);
            statement3.execute();
            ResultSet generatedKeys;
            generatedKeys = statement3.getGeneratedKeys();
            if (generatedKeys.next()) {
                SWUp_Rec.IDZBFileDownloadSession = generatedKeys.getLong(1);
                generatedKeys.close();
            }
            statement3.close();
        }
        rs2.close();
        statement2.close();

        SWUp_Rec.LastCommandSent_Date = now;

        QueryString = "UPDATE swupprocess SET Stato=1,LastCheck=?,LastSentChunk_Date=null,IDCommandDownload=?"
                + ",IDCommandSWUP=null,IDZBFileDownloadSession=?,LastCommandSent_Date=? WHERE IDSWUpProcess=? ";
        statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement1.setTimestamp(1, now);
        statement1.setLong(2, SWUp_Rec.IDCommandDownload);
        statement1.setLong(3, SWUp_Rec.IDZBFileDownloadSession);
        statement1.setTimestamp(4, SWUp_Rec.LastCommandSent_Date);
        statement1.setLong(5, SWUp_Rec.IDSWUpProcess);
        statement1.execute();
        statement1.close();

        System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato DoStato0_1 IDCommandDownload=" + SWUp_Rec.IDCommandDownload + " IDZBFileDownloadSession=" + SWUp_Rec.IDZBFileDownloadSession + " transito in stato 1");

        return 0;
    }

    // esiste un comando di pre aggiornamento
    int DoStato0_5(ZB_SWUpdate_Record SWUp_Rec) throws SQLException {
        PreparedStatement statement, statement1, statement2, statement3;
        ResultSet rs;
        Timestamp now = new Timestamp((new java.util.Date()).getTime());
        // identificazione del campo IDCommand
        ResultSet rs2;
        String QueryString = "Select IDZBCommand FROM ZBCommand "
                + " where IDBlackBox=? and Stato =0 and Command LIKE ?"
                + " order by IDZBCommand Desc Limit 1";
        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement2.setLong(1, SWUp_Rec.IDBlackBox);
        statement2.setString(2, SWUp_Rec.CommandPre);
        rs2 = statement2.executeQuery();
        if (rs2.next()) {
            SWUp_Rec.IDCommandPre = rs2.getLong("IDZBCommand");
        } else {
            QueryString = "INSERT INTO ZBCommand (IDBlackBox, Command, Stato) VALUES(?,?,0) ";
            statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement3.setLong(1, SWUp_Rec.IDBlackBox);
            statement3.setString(2, SWUp_Rec.CommandPre);
            statement3.execute();
            ResultSet generatedKeys;
            generatedKeys = statement3.getGeneratedKeys();
            if (generatedKeys.next()) {
                SWUp_Rec.IDCommandPre = generatedKeys.getLong(1);
                generatedKeys.close();
            }
            statement3.close();
        }
        rs2.close();
        statement2.close();

        SWUp_Rec.LastCommandSent_Date = now;

        QueryString = "UPDATE swupprocess SET Stato=5,LastCheck=?,IDCommandPre=?"
                + ",LastCommandSent_Date=? WHERE IDSWUpProcess=? ";
        statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement1.setTimestamp(1, now);
        statement1.setLong(2, SWUp_Rec.IDCommandPre);
        statement1.setTimestamp(3, SWUp_Rec.LastCommandSent_Date);
        statement1.setLong(4, SWUp_Rec.IDSWUpProcess);
        statement1.execute();
        statement1.close();
        return 0;
    }

    /**
     * Processo che verifica il recuper del comando di download
     *
     * @param SWUp_Rec
     * @return
     */
    int CheckStato1(ZB_SWUpdate_Record SWUp_Rec) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if ((now.getTime() - SWUp_Rec.LastCheck.getTime()) > 20000) { // esegue la verifica ogni 20 sec 
            try {
                PreparedStatement statement, statement1, statement2, statement3;
                ResultSet rs, rs2;
                int StatoSWUP = 1;
                String QueryString = "Select IDZBCommand FROM ZBCommand "
                        + "where IDZBCommand=? and Stato>=1";
                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement2.setLong(1, SWUp_Rec.IDCommandDownload);
                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    StatoSWUP = 2;
                    SWUp_Rec.LastCommandSent_Date = now;
                    System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 1 ... comando di download acquisito: transito in stato 2");
                } else if (SWUp_Rec.LastCommandSent_Date != null) {
                    if ((now.getTime() - SWUp_Rec.LastCommandSent_Date.getTime()) > (480 * 60 * 1000)) {
                        StatoSWUP = 51;   // errore per timeout del comando di SW update
                    }
                } else {
                    System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 1 ... attesa recepimento del comando di download");
                }

                QueryString = "UPDATE swupprocess SET Stato=?,LastCheck=?, LastCommandSent_Date=? ,LastSentChunk_Date=?"
                        + " WHERE IDSWUpProcess=? ";
                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement1.setInt(1, StatoSWUP);
                statement1.setTimestamp(2, now);
                statement1.setTimestamp(3, SWUp_Rec.LastCommandSent_Date);
                statement1.setTimestamp(4, SWUp_Rec.LastCommandSent_Date);
                statement1.setLong(5, SWUp_Rec.IDSWUpProcess);
                statement1.execute();
                statement1.close();
            } catch (SQLException ex) {
                Logger.getLogger(ZB_SWUpdateProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }

    /**
     * Processo lo stato della procedura di SWupdate verificando i dati sulla
     * tabella ZBFileDownloadSession
     *
     * @param SWUp_Rec
     * @return
     */
    int CheckStato2(ZB_SWUpdate_Record SWUp_Rec) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if ((now.getTime() - SWUp_Rec.LastCheck.getTime()) > 20000) { // esegue la verifica ogni 20 sec 
            try {
                PreparedStatement statement, statement1, statement2, statement3;
                ResultSet rs, rs2;
                String QueryString = "Select DS.IDZBFileDownloadSession, DS.IDBlackBox, DS.Stato, DS.LastSent_Date, DW.FileName, "
                        + " DS.LastSentChunk FROM ZBFileDownloadSession DS "
                        + " LEFT JOIN ZBFileDownload Dw on Dw.IDZBFileDownload=DS.IDZBFileDownload "
                        + " where IDZBFileDownloadSession=?";
                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement2.setLong(1, SWUp_Rec.IDZBFileDownloadSession);
                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    int Stato = rs2.getInt("Stato");
                    int StatoSWUP = 2;
                    Timestamp LastSentChunk_Date = rs2.getTimestamp("LastSent_Date");
                    if (Stato == 1) {
                        StatoSWUP = 3;
                        QueryString = "INSERT INTO ZBCommand (IDBlackBox, Command, Stato) "
                                + " VALUES(?,?,0) ";
                        statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                        statement3.setLong(1, SWUp_Rec.IDBlackBox);
                        statement3.setString(2, "SWU-Cmd:2;0:/SW/" + rs2.getString("FileName"));
                        statement3.execute();
                        ResultSet generatedKeys;
                        generatedKeys = statement3.getGeneratedKeys();
                        if (generatedKeys.next()) {
                            SWUp_Rec.IDCommandSWUP = generatedKeys.getLong(1);
                            generatedKeys.close();
                        }
                        statement3.close();
                        SWUp_Rec.LastCommandSent_Date = now;
                        System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 2 ... inserito comando di update: transito in stato 3");
                    } else if (LastSentChunk_Date == null) {
                        StatoSWUP = 2;
                    } else if ((now.getTime() - LastSentChunk_Date.getTime()) > (120 * 60 * 1000)) {
                        StatoSWUP = 52;   // errore per timeout                        
                    }


                    QueryString = "UPDATE swupprocess SET Stato=?,LastCheck=?,LastSentChunk_Date=?,"
                            + "IDCommandSWUP=?,LastCommandSent_Date=? WHERE IDSWUpProcess=? ";
                    statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                    statement1.setInt(1, StatoSWUP);
                    statement1.setTimestamp(2, now);
                    statement1.setTimestamp(3, LastSentChunk_Date);
                    statement1.setLong(4, SWUp_Rec.IDCommandSWUP);
                    statement1.setTimestamp(5, SWUp_Rec.LastCommandSent_Date);
                    statement1.setLong(6, SWUp_Rec.IDSWUpProcess);
                    statement1.execute();
                    statement1.close();

                    System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 2 ChunkNum=" + rs2.getInt("LastSentChunk")
                            + " LastSentChunk_Date=" + LastSentChunk_Date + " IDZBFileDownloadSession=" + SWUp_Rec.IDZBFileDownloadSession);
                }
                statement2.close();
                rs2.close();
            } catch (SQLException ex) {
                Logger.getLogger(ZB_SWUpdateProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }

    /**
     * Processo che verifica l'invio del comando di SWUpdate
     *
     * @param SWUp_Rec
     * @return
     */
    int CheckStato3(ZB_SWUpdate_Record SWUp_Rec) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if ((now.getTime() - SWUp_Rec.LastCheck.getTime()) > 20000) { // esegue la verifica ogni 20 sec 
            try {
                PreparedStatement statement, statement1, statement2, statement3;
                ResultSet rs, rs2;
                int StatoSWUP = 3;
                String QueryString = "Select IDZBCommand FROM ZBCommand "
                        + "where IDZBCommand=? and Stato =1";
                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement2.setLong(1, SWUp_Rec.IDCommandSWUP);
                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    StatoSWUP = 4;
                    System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 3 ... Comando Update acquisito: transito in stato 4");
                } else if ((now.getTime() - SWUp_Rec.LastCommandSent_Date.getTime()) > (480 * 60 * 1000)) {
                    StatoSWUP = 53;   // errore per timeout del comando di SW update                    
                }

                QueryString = "UPDATE swupprocess SET Stato=?,LastCheck=?"
                        + " WHERE IDSWUpProcess=? ";
                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement1.setInt(1, StatoSWUP);
                statement1.setTimestamp(2, now);
                statement1.setLong(3, SWUp_Rec.IDSWUpProcess);
                statement1.execute();
                statement1.close();
                System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 3 ... Attesa trasferimento comando di SWUpdate");
            } catch (SQLException ex) {
                Logger.getLogger(ZB_SWUpdateProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }

    /**
     * Processo che verfica l'invio di record info successivi all'aggiornamento
     * SW
     *
     * @param SWUp_Rec
     * @return
     */
    int CheckStato4(ZB_SWUpdate_Record SWUp_Rec) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if ((now.getTime() - SWUp_Rec.LastCheck.getTime()) > 120000) { // esegue la verifica ogni 20 sec 
            try {
                PreparedStatement statement, statement1, statement2, statement3;
                ResultSet rs, rs2;
                int StatoSWUP = 4;
                String SWVersion = "---";
                String QueryString = "select IDZBInfo,IDBlackBox,MaiorVersion,MinorVersion,BtimeStamp from ZBInfo "
                        + "where IDBlackBox=? and BtimeStamp>=?";
                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement2.setLong(1, SWUp_Rec.IDBlackBox);
                statement2.setTimestamp(2, SWUp_Rec.LastSentChunk_Date);
                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    SWVersion = "" + rs2.getInt("MaiorVersion") + "." + rs2.getInt("MinorVersion");
                    if (SWUp_Rec.IDCommandPost > 0 || SWUp_Rec.CommandPost != null) {
                        StatoSWUP = 6;
                        QueryString = "INSERT INTO ZBCommand (IDBlackBox, Command, Stato) VALUES(?,?,0) ";
                        statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                        statement3.setLong(1, SWUp_Rec.IDBlackBox);
                        statement3.setString(2, SWUp_Rec.CommandPost);
                        statement3.execute();
                        ResultSet generatedKeys;
                        generatedKeys = statement3.getGeneratedKeys();
                        if (generatedKeys.next()) {
                            SWUp_Rec.IDCommandPost = generatedKeys.getLong(1);
                            generatedKeys.close();
                        }
                        statement3.close();
                        QueryString = "UPDATE swupprocess SET IDCommandPost=? WHERE IDSWUpProcess=? ";
                        statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                        statement1.setLong(1, SWUp_Rec.IDCommandPost);
                        statement1.setLong(2, SWUp_Rec.IDSWUpProcess);
                        statement1.execute();
                        statement1.close();
                        System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 4 ... inviato comando di PostUpdate; versione: " + SWVersion);
                    } else {
                        StatoSWUP = 20;
                        System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 4 ... procedura completata: versione " + SWVersion);
                    }
                } else if ((now.getTime() - SWUp_Rec.LastCommandSent_Date.getTime()) > (480 * 60 * 1000)) {
                    StatoSWUP = 54;   // errore per timeout del comando di SW update                    
                }

                QueryString = "UPDATE swupprocess SET Stato=?,LastCheck=?, FinalSWVersion=?"
                        + " WHERE IDSWUpProcess=? ";
                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement1.setInt(1, StatoSWUP);
                statement1.setTimestamp(2, now);
                statement1.setString(3, SWVersion);
                statement1.setLong(4, SWUp_Rec.IDSWUpProcess);
                statement1.execute();
                statement1.close();
                System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 4 ... Attesa del messaggio di info sulla versione");
            } catch (SQLException ex) {
                Logger.getLogger(ZB_SWUpdateProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }

    /**
     * Processo che verfica l'invio del comando di pre configurazione
     *
     * @param SWUp_Rec
     * @return
     */
    int CheckStato5(ZB_SWUpdate_Record SWUp_Rec) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if ((now.getTime() - SWUp_Rec.LastCheck.getTime()) > 120000) { // esegue la verifica ogni 20 sec 
            try {
                PreparedStatement statement, statement1, statement2, statement3;
                ResultSet rs, rs2;
                int StatoSWUP = 5;
                String QueryString = "Select IDZBCommand FROM ZBCommand "
                        + "where IDZBCommand=? and Stato>=1";
                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement2.setLong(1, SWUp_Rec.IDCommandPre);
                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 5 ... comando di pre aggiornamento acquisito: transito in stato 1");
                    StatoSWUP = 1;
                    DoStato0_1(SWUp_Rec);
                    SWUp_Rec.LastCommandSent_Date = now;
//                    System.out.println("SWUp IDBB="+SWUp_Rec.IDBlackBox+" Stato 5 ... comando di pre aggiornamento acquisito: transito in stato 1");
                } else if (SWUp_Rec.LastCommandSent_Date != null) {
                    if ((now.getTime() - SWUp_Rec.LastCommandSent_Date.getTime()) > (480 * 60 * 1000)) {
                        StatoSWUP = 55;   // errore per timeout del comando di SW update
                    }
                }

                QueryString = "UPDATE swupprocess SET Stato=?,LastCheck=?, LastCommandSent_Date=?"
                        + " WHERE IDSWUpProcess=? ";
                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement1.setInt(1, StatoSWUP);
                statement1.setTimestamp(2, now);
                statement1.setTimestamp(3, SWUp_Rec.LastCommandSent_Date);
                statement1.setLong(4, SWUp_Rec.IDSWUpProcess);
                statement1.execute();
                statement1.close();
//                System.out.println("SWUp IDBB="+SWUp_Rec.IDBlackBox+" Stato 5 ... attesa recepimento del comando di pre-aggiornamento");
            } catch (SQLException ex) {
                Logger.getLogger(ZB_SWUpdateProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }

    /**
     * Processo che verfica l'invio del comando di post configurazione
     *
     * @param SWUp_Rec
     * @return
     */
    int CheckStato6(ZB_SWUpdate_Record SWUp_Rec) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if ((now.getTime() - SWUp_Rec.LastCheck.getTime()) > 120000) { // esegue la verifica ogni 20 sec 
            try {
                PreparedStatement statement1, statement2;
                ResultSet rs2;
                int StatoSWUP = 6;
                String QueryString = "Select IDZBCommand FROM ZBCommand "
                        + "where IDZBCommand=? and Stato>=1";
                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement2.setLong(1, SWUp_Rec.IDCommandPost);
                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    StatoSWUP = 20;
                    SWUp_Rec.LastCommandSent_Date = now;
                    System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 6 ... comando di post aggiornamento acquisito: transito in stato 20");
                } else if (SWUp_Rec.LastCommandSent_Date != null) {
                    if ((now.getTime() - SWUp_Rec.LastCommandSent_Date.getTime()) > (480 * 60 * 1000)) {
                        StatoSWUP = 56;   // errore per timeout del comando di SW update
                    }
                }

                QueryString = "UPDATE swupprocess SET Stato=?,LastCheck=?, LastCommandSent_Date=?"
                        + " WHERE IDSWUpProcess=? ";
                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement1.setInt(1, StatoSWUP);
                statement1.setTimestamp(2, now);
                statement1.setTimestamp(3, SWUp_Rec.LastCommandSent_Date);
                statement1.setLong(4, SWUp_Rec.IDSWUpProcess);
                statement1.execute();
                statement1.close();
//                System.out.println("SWUp IDBB="+SWUp_Rec.IDBlackBox+" Stato 6 ... attesa recepimento del comando di post-aggiornamento");
            } catch (SQLException ex) {
                Logger.getLogger(ZB_SWUpdateProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }
}
