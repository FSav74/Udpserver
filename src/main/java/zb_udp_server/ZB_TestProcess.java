/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.zbox;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class ZB_TestProcess {

    class TestFile {

        long IDZBFileUpload;
        String FileData;
    }

    class TestReport {

        long IDZBTest;
        long IDBlackBox;
        Timestamp DateSet;
        Timestamp DateSent;
        long IDZBCommand;
        int Stato;
        long IDZBFileUpload;
        int Response;
        Timestamp DateComplete;
        String ResponseString;
        int CStato;
    }
    DBAdminClass DBAdmin;
    ConfClass Conf;
    LogsClass Log;
    static int STATO_INVIO_TEST = 1;
    static int STATO_ATTESA_RICEZIONE_COMANDO_TEST = 2;
    static int STATO_ATTESA_FILE_TEST = 3;
    static int STATO_TEST_VERIFICA_COMPLETATA = 4;
    static int STATO_FAIL = 50;
    Timestamp TimeLimit;

    public ZB_TestProcess() {
//        receivePacket=Received;
//        serverSocket=Socket;

        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();

    }

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin) {
        DBAdmin = LDBAdmin;

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
    public int RunZBTestProcess() {
        Statement statement, statement1;
        Timestamp now = new Timestamp((new java.util.Date()).getTime());
        TimeLimit = new Timestamp(now.getTime() - (7 * 24 * 60 * 60 * 1000));     // 7 giorni prima

        ResultSet rs, rs1;
        DBAdmin.CheckConnection();
        try {
            if (DBAdmin.DbAdminConn.isClosed()) {
                return 0;
            }
        } catch (SQLException ex) {
            return 0;
        }

        ArrayList<zbox> ZBs = GetZBoxToTest(DBAdmin.DbAdminConn);
        if (ZBs != null) {
            for (zbox ZB : ZBs) {
                //System.out.println(name);
                TestReport T = GetLastTest(DBAdmin.DbAdminConn, ZB);

                if (T != null) {
                    if (T.Stato == STATO_TEST_VERIFICA_COMPLETATA) {
                        if (CheckStato4(DBAdmin.DbAdminConn, ZB, T) == 1) {
                            continue;
                        }
                    }

                    if (T.Stato == STATO_ATTESA_FILE_TEST) {
                        if (CheckStato3(DBAdmin.DbAdminConn, ZB, T) == 1) {
                            continue;
                        }
                    }

                    if (T.Stato == STATO_ATTESA_RICEZIONE_COMANDO_TEST) {
                        if (CheckStato2(DBAdmin.DbAdminConn, ZB, T) == 1) {
                            continue;
                        }
                    }
                }
                if (CheckStato1(DBAdmin.DbAdminConn, ZB) == 1) {
                    continue;
                }

            }
        }

        return 0;
    }

    ArrayList<zbox> GetZBoxToTest(Connection conn) {
        Statement statement;
        PreparedStatement pstatement1;
        ResultSet rs, rs1;
        ArrayList<zbox> A;

        try {
            // esegue la query al DB
            statement = conn.createStatement();
//            pstatement1 = conn.createStatement();

            String QueryString = "SELECT IDBlackBox FROM blackbox_debug.blackbox where IDAzienda=30 ORDER BY LastContact Desc,IDBlackBox LIMIT 10000 ";
            //String QueryString ="SELECT * FROM blackbox_debug.blackbox where IDBlackBox=8152";

            rs = statement.executeQuery(QueryString);
            A = new ArrayList<zbox>();
            while (rs.next()) {
                zbox ZB = new zbox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");

                A.add(ZB);
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return A;
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
                // identificazione del campo IDCommand
                ResultSet rs2;
                QueryString = "Select IDZBCommand FROM ZBCommand "
                        + " WHERE IDBlackBox=? and Stato=0 AND Command LIKE 'GPR-Dwl:ON'"
                        + " ORDER BY IDZBCommand DESC LIMIT 1";
                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                statement2.setLong(1, SWUp_Rec.IDBlackBox);
                rs2 = statement2.executeQuery();
                if (rs2.next()) {
                    SWUp_Rec.IDCommandDownload = rs2.getLong("IDZBCommand");
                } else {
                    QueryString = "INSERT INTO ZBCommand (IDBlackBox, Command, Stato)"
                            + " VALUES(?,'GPR-Dwl:ON',0) ";
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
                System.out.println("SWUp IDBB=" + SWUp_Rec.IDBlackBox + " Stato 0 IDCommandDownload=" + SWUp_Rec.IDCommandDownload + " IDZBFileDownloadSession=" + SWUp_Rec.IDZBFileDownloadSession);
            }
            statement.close();
            rs.close();
        }
        return 0;
    }

    /**
     * Processo che verifica il recuper del comando di download
     *
     * @param SWUp_Rec
     * @return
     */
    int CheckStato1(Connection conn, zbox ZB) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());
        PreparedStatement pstatement1;
        ResultSet rs, rs1;
        long IDZBCommand = 0;

//        String QueryString1 ="SELECT T.*,C.Stato CStato FROM zbtest T "+
//                        " LEFT JOIN ZBCommand C on C.IDZBCommand=T.IDZBCommand"+
//                        " WHERE T.IDBlackBox="+ZB.IDBlackBox+" AND T.Stato="+STATO_ATTESA_RICEZIONE_COMANDO_TEST+
//                        " order by T.IDZBTest DESC LIMIT 1";
        try {
//                pstatement1= conn.prepareStatement(QueryString1);
            try {
                String QueryString = "INSERT INTO ZBCommand (IDBlackBox, Command, Stato) VALUES(?,'ZB-Test:;3',0) ";
                pstatement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                pstatement1.setLong(1, ZB.IDBlackBox);
                pstatement1.execute();
                ResultSet generatedKeys;
                generatedKeys = pstatement1.getGeneratedKeys();
                if (generatedKeys.next()) {
                    IDZBCommand = generatedKeys.getLong(1);
                    generatedKeys.close();
                }
                pstatement1.close();

                QueryString = "INSERT INTO ZBTest (IDBlackBox,IDZBCommand, Stato) VALUES(?,?, 2) ";
                pstatement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                pstatement1.setLong(1, ZB.IDBlackBox);
                pstatement1.setLong(2, IDZBCommand);
                pstatement1.execute();

                pstatement1.close();
                System.out.println("ZBTest IDBB=" + ZB.IDBlackBox + " Stato 1 ... transito in stato 2");

            } finally {
//              pstatement1.close();    
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }

//        if ((now.getTime() - SWUp_Rec.LastCheck.getTime())>20000) { // esegue la verifica ogni 20 sec 
//            try {
//                PreparedStatement statement,statement1,statement2,statement3;
//                ResultSet rs,rs2;
//                int StatoSWUP=1;
//                String QueryString = "Select IDZBCommand FROM ZBCommand "
//                        + "where IDZBCommand=? and Stato =1";
//                statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
//                statement2.setLong(1, SWUp_Rec.IDCommandDownload);
//                rs2=statement2.executeQuery();
//                if (rs2.next()) {
//                    StatoSWUP = 2;
//                    SWUp_Rec.LastCommandSent_Date=now;
//                    System.out.println("SWUp IDBB="+SWUp_Rec.IDBlackBox+" Stato 1 ... comando di download acquisito: transito in stato 2");
//                } else if (SWUp_Rec.LastCommandSent_Date!=null) {
//                    if ((now.getTime() - SWUp_Rec.LastCommandSent_Date.getTime())>(480*60*1000)) {
//                        StatoSWUP =51;   // errore per timeout del comando di SW update
//                    }
//                }
//                
//                QueryString = "UPDATE swupprocess SET Stato=?,LastCheck=?, LastCommandSent_Date=? ,LastSentChunk_Date=?"
//                        + " WHERE IDSWUpProcess=? ";
//                statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
//                statement1.setInt(1, StatoSWUP);
//                statement1.setTimestamp(2, now);
//                statement1.setTimestamp(3, SWUp_Rec.LastCommandSent_Date);
//                statement1.setTimestamp(4, SWUp_Rec.LastCommandSent_Date);
//                statement1.setLong(5, SWUp_Rec.IDSWUpProcess);
//                statement1.execute();                
//                statement1.close();
//                System.out.println("SWUp IDBB="+SWUp_Rec.IDBlackBox+" Stato 1 ... attesa recepimento del comando di download");
//            } catch (SQLException ex) {
//                Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
        return 0;
    }

    /**
     * Processo lo stato della procedura di SWupdate verificando i dati sulla
     * tabella ZBFileDownloadSession
     *
     * @param SWUp_Rec
     * @return 0 se nessuna operazione è stata effettuata 1 se la ZB è stata
     * trattata
     */
    int CheckStato2(Connection conn, zbox ZB, TestReport T) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        if (T.CStato == 0) {
            return 1;
        } else if (T.CStato == 1) {
            SetTestStato(T.IDZBTest, STATO_ATTESA_FILE_TEST);
            SetTestDateSent(T.IDZBTest, now);
            System.out.println("ZBTest IDBB=" + ZB.IDBlackBox + " Stato 2 ... comando di download acquisito: transito in stato 3");
            return 1;
        } else if (T.DateSet.getTime() < TimeLimit.getTime()) {
            SetTestStato(T.IDZBTest, STATO_FAIL);
            System.out.println("ZBTest IDBB=" + ZB.IDBlackBox + " Stato 2 ... timeout: transito in stato 50");
            return 1;
        } else {
            return 1;
        }


//        PreparedStatement pstatement1;
//        ResultSet rs,rs1;
//        
//        String QueryString1 ="SELECT T.*,C.Stato CStato FROM zbtest T "+
//                        " LEFT JOIN ZBCommand C on C.IDZBCommand=T.IDZBCommand"+
//                        " WHERE T.IDBlackBox="+ZB.IDBlackBox+" AND T.Stato="+STATO_ATTESA_RICEZIONE_COMANDO_TEST+
//                        " order by T.IDZBTest DESC LIMIT 1";
//         try {
//            pstatement1= conn.prepareStatement(QueryString1);
//
//            try {
//                rs1 = pstatement1.executeQuery();
//                if (rs1.next()) {
//                    // se il comando è stato acquisito
//                        // imposto il DateSent
//                        // transito in stato 3
//                    // se il DateSet è vecchio
//                        // disabilito il comando inviato
//                        // pongo il test in fail
//                    // se il DateSent è piu' recente ritorno
//                    int CStato=rs1.getInt("CStato");
//                    int IDZBTest=rs1.getInt("IDZBTest");
//                    Timestamp DateSet=rs1.getTimestamp("DateSet");
//                    rs1.close();
//                    if (CStato==0) {
//                        return 1;
//                    } else if (CStato==1) {
//                        SetTestStato(IDZBTest, STATO_ATTESA_FILE_TEST);
//                        SetTestDateSent(IDZBTest, now);
//                        System.out.println("ZBTest IDBB="+ZB.IDBlackBox+" Stato 2 ... comando di download acquisito: transito in stato 3");
//                        return 1;
//                    } else if (DateSet.getTime()<TimeLimit.getTime()) {
//                        SetTestStato(IDZBTest, STATO_FAIL);
//                        System.out.println("ZBTest IDBB="+ZB.IDBlackBox+" Stato 2 ... timeout: transito in stato 50");
//                        return 1;
//                    } else {
//                        return 1;
//                    }
//                } else {
//                    rs1.close();
//                    return 0;
//                }
//            } finally {
//                pstatement1.close();    
//            }
//        } catch (SQLException ex) {
//            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        
//        return 0;
    }

    /**
     * Processo che verifica l'invio del comando di SWUpdate
     *
     * @param SWUp_Rec
     * @return 0 se nessuna operazione è stata effettuata 1 se la ZB è stata
     * trattata
     */
    int CheckStato3(Connection conn, zbox ZB, TestReport T) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());
        TestFile FileData = GetFile(ZB, T.DateSent);
        if (FileData != null) {
            //Verifica file
            //setta nuovo stato
            VerifyFile(ZB, T.IDZBTest, FileData);
            SetTestStato(T.IDZBTest, STATO_TEST_VERIFICA_COMPLETATA);
            SetTestIDZBFileUpload(T.IDZBTest, FileData.IDZBFileUpload);
            SetTestDateComplete(T.IDZBTest, now);
            System.out.println("ZBTest IDBB=" + ZB.IDBlackBox + " Stato 3 ... Test ricevuto: transito in stato 4");
            return 1;

        } else if (T.DateSent.getTime() < TimeLimit.getTime()) {
            SetTestStato(T.IDZBTest, STATO_FAIL);
            System.out.println("ZBTest IDBB=" + ZB.IDBlackBox + " Stato 3 ... Timeout: transito in stato 50");
            return 1;
        } else {
            return 1;
        }
//        PreparedStatement pstatement1;
//        ResultSet rs,rs1;
//        
//        String QueryString1 ="SELECT * FROM blackbox_debug.zbtest where IDBlackBox="+ZB.IDBlackBox+
////                        " WHERE Stato="+STATO_ATTESA_FILE_TEST+" and DateSent> DATE_SUB(NOW(),INTERVAL 7 DAY)"+
//                        " AND Stato="+STATO_ATTESA_FILE_TEST+
//                        " order by IDZBTest DESC LIMIT 1";
//         try {
//            pstatement1= conn.prepareStatement(QueryString1);
//
//            try {
//                rs1 = pstatement1.executeQuery();
//                if (rs1.next()) {
//                    // se il file esiste
//                        // lo verifico e transito in stato 4
//                    // se il DateSent è vecchio
//                        // pongo il test in fail
//                    // se il DateSent è piu' recente ritorno
//                    long IDZBTest=rs1.getLong("IDZBTest");
//                    Timestamp DateSent=rs1.getTimestamp("DateSent");
//                    TestFile FileData=GetFile(ZB, DateSent);
//                    
//                    rs1.close();
//                    if (FileData!= null) {
//                        //Verifica file
//                        //setta nuovo stato
//                        VerifyFile(ZB, IDZBTest, FileData);
//                        SetTestStato(IDZBTest, STATO_TEST_VERIFICA_COMPLETATA);
//                        SetTestIDZBFileUpload(IDZBTest,FileData.IDZBFileUpload);
//                        SetTestDateComplete(IDZBTest,now);
//                        System.out.println("ZBTest IDBB="+ZB.IDBlackBox+" Stato 3 ... Test ricevuto: transito in stato 4");
//                        return 1;
//
//                    } else if (DateSent.getTime()<TimeLimit.getTime()) {
//                        SetTestStato(IDZBTest, STATO_FAIL);
//                        System.out.println("ZBTest IDBB="+ZB.IDBlackBox+" Stato 3 ... Timeout: transito in stato 50");
//                        return 1;
//                    } else {
//                        return 1;
//                    }          
//                } else {
//                    rs1.close();
//                    return 0;
//                }
//            } finally {
//                pstatement1.close();    
//            }
//        } catch (SQLException ex) {
//            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return 0;
    }

    /**
     * Processo che verfica l'invio di record info successivi all'aggiornamento
     * SW
     *
     * @param SWUp_Rec
     * @return 0 se nessuna operazione è stata effettuata 1 se la ZB è stata
     * trattata
     */
    int CheckStato4(Connection conn, zbox ZB, TestReport T) {
        Timestamp now = new Timestamp((new java.util.Date()).getTime());
//        PreparedStatement pstatement1;
//        ResultSet rs1;
//        
//        String QueryString1 ="SELECT * FROM blackbox_debug.zbtest where IDBlackBox="+ZB.IDBlackBox+
////                        " AND Stato="+STATO_TEST_VERIFICA_COMPLETATA+" and DateComplete> DATE_SUB(NOW(),INTERVAL 7 DAY)"+
//                        " AND Stato="+STATO_TEST_VERIFICA_COMPLETATA+
//                        " ORDER BY IDZBTest DESC LIMIT 1";
//        try {
//            pstatement1= conn.prepareStatement(QueryString1);
//                
//            try {
//                rs1 = pstatement1.executeQuery();
//                if (rs1.next()) {
//                    
//                    Timestamp DateComplete=rs1.getTimestamp("DateComplete");
//                    long IDZBTest=rs1.getLong("IDZBTest");
//                    rs1.close();
//                    if (DateComplete==null){
//                        SetTestDateComplete(IDZBTest,now);
//                        return 1;
//                    } else if (DateComplete.getTime()<TimeLimit.getTime()) {
//                        return 0;
//                    } else {
//                        return 1;
//                    }                   
//                } else {
//                    rs1.close();
//                    return 0;
//                }
//            } finally {
//                pstatement1.close();    
//            }
//        } catch (SQLException ex) {
//            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return 0;
        if (T.DateComplete == null) {
            SetTestDateComplete(T.IDZBTest, now);
            return 1;
        } else if (T.DateComplete.getTime() < TimeLimit.getTime()) {
            return 0;
        } else {
            return 1;
        }
    }

    TestReport GetLastTest(Connection conn, zbox ZB) {
        TestReport ret = null;
        Timestamp now = new Timestamp((new java.util.Date()).getTime());

        PreparedStatement pstatement1;
        ResultSet rs1;

        String QueryString1 = "SELECT T.*,C.Stato CStato FROM blackbox_debug.zbtest T"
                + " LEFT JOIN ZBCommand C on C.IDZBCommand=T.IDZBCommand"
                + " where T.IDBlackBox=" + ZB.IDBlackBox
                + //                        " WHERE Stato="+STATO_ATTESA_FILE_TEST+" and DateSent> DATE_SUB(NOW(),INTERVAL 7 DAY)"+
                //                        " AND Stato="+STATO_ATTESA_FILE_TEST+
                " order by T.IDZBTest DESC LIMIT 1";
        try {
            pstatement1 = conn.prepareStatement(QueryString1);

            try {
                rs1 = pstatement1.executeQuery();
                if (rs1.next()) {
                    ret = new TestReport();
                    ret.IDZBTest = rs1.getLong("IDZBTest");
                    ret.IDBlackBox = rs1.getLong("IDBlackBox");
                    ret.DateSet = rs1.getTimestamp("DateSet");
                    ret.DateSent = rs1.getTimestamp("DateSent");
                    ret.IDZBCommand = rs1.getLong("IDZBCommand");
                    ret.Stato = rs1.getInt("Stato");
                    ret.IDZBFileUpload = rs1.getLong("IDZBFileUpload");
                    ret.Response = rs1.getInt("Response");
                    ret.DateComplete = rs1.getTimestamp("DateComplete");
                    ret.ResponseString = rs1.getString("ResponseString");
                    ret.CStato = rs1.getInt("CStato");
                }
                rs1.close();
            } finally {
                pstatement1.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ret;
    }

    void VerifyFile(zbox ZB, long IDZBTest, TestFile TF) {
        int Response = 0;
        String ResponseString = "";
        if (TF.FileData.contains("CAL: NOK")) {
            Response = 2;
            ResponseString += "CAL: NOK;";
        } else {
            Response = 1;
        }
        if (TF.FileData.contains("GPS: NOK")) {
            ResponseString += "GPS: NOK;";
        }
        if (TF.FileData.contains("CAN: NOK")) {
            ResponseString += "CAN: NOK;";
        }
        if (TF.FileData.contains("ACC: NOK")) {
            ResponseString += "ACC: NOK;";
        }
        if (TF.FileData.contains("USD: NOK")) {
            ResponseString += "USD: NOK;";
        }

        // 0 non noto; 1 ok; 2 nok
        SetTestResponse(IDZBTest, Response, ResponseString);
    }

    TestFile GetFile(zbox ZB, Timestamp T) {
        PreparedStatement pstatement1;
        ResultSet rs, rs1;

        String QueryString1 = "SELECT * FROM blackbox_debug.zbfileupload WHERE IDBlackBox=" + ZB.IDBlackBox
                + " AND FileType=6 AND Stato=1 AND FileName like \"0:/Z-Box_TestRes.txt.tmp%\" AND"
                + " FileTimeStamp>? ORDER BY IDZBFileUpload DESC LIMIT 1";
        try {
            pstatement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString1);


            try {
                pstatement1.setTimestamp(1, T);
                rs1 = pstatement1.executeQuery();
                if (rs1.next()) {
                    TestFile Ret = new TestFile();
                    Ret.IDZBFileUpload = rs1.getLong("IDZBFileUpload");
                    Ret.FileData = rs1.getString("FileData");
                    rs1.close();
                    return Ret;
                } else {
                    rs1.close();
                    return null;
                }
            } finally {
                pstatement1.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    void SetTestStato(long IDZBTest, int Stato) {
        PreparedStatement statement1;
//        ResultSet rs,rs1;

        String QueryString = "UPDATE zbtest SET Stato=? WHERE IDZBTest=? ";
        try {
            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            statement1.setInt(1, Stato);
            statement1.setLong(2, IDZBTest);
            try {
                statement1.execute();
            } finally {
                statement1.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    void SetTestIDZBFileUpload(long IDZBTest, long IDZBFileUpload) {
        PreparedStatement statement1;

        String QueryString = "UPDATE zbtest SET IDZBFileUpload=? WHERE IDZBTest=? ";
        try {
            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            statement1.setLong(1, IDZBFileUpload);
            statement1.setLong(2, IDZBTest);
            try {
                statement1.execute();
            } finally {
                statement1.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    void SetTestDateSent(long IDZBTest, Timestamp DateSent) {
        PreparedStatement statement1;

        String QueryString = "UPDATE zbtest SET DateSent=? WHERE IDZBTest=? ";
        try {
            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            statement1.setTimestamp(1, DateSent);
            statement1.setLong(2, IDZBTest);
            try {
                statement1.execute();
            } finally {
                statement1.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    void SetTestDateComplete(long IDZBTest, Timestamp DateComplete) {
        PreparedStatement statement1;

        String QueryString = "UPDATE zbtest SET DateComplete=? WHERE IDZBTest=? ";
        try {
            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            statement1.setTimestamp(1, DateComplete);
            statement1.setLong(2, IDZBTest);
            try {
                statement1.execute();
            } finally {
                statement1.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    void SetTestResponse(long IDZBTest, int Response, String ResponseString) {
        PreparedStatement statement1;

        String QueryString = "UPDATE zbtest SET Response=?,ResponseString=? WHERE IDZBTest=? ";
        try {
            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            statement1.setInt(1, Response);
            statement1.setString(2, ResponseString);
            statement1.setLong(3, IDZBTest);
            try {
                statement1.execute();
            } finally {
                statement1.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ZB_TestProcess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
