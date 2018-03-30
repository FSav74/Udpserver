/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.*;
import com.viacom.zbox.execeptions.ExceptionCRCError;
import com.viacom.zbox.execeptions.ExceptionInvalidRecordLenght;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.security.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;

/**
 *
 * @author Luca
 */
public class ZB_UDP_Comunication extends Thread {

    static byte GPRS_COMMAND_HELO = (byte) 0x01;
    static byte GPRS_COMMAND_HELO_OK = (byte) 0x05;
    static byte GPRS_COMMAND_OK = (byte) 0x02;
    static byte GPRS_COMMAND_NOK = (byte) 0x03;
    static byte GPRS_COMMAND_PUSH_NOTIFY = (byte) 0x06;
    static byte GPRS_COMMAND_PUSH_NOTIFY_OK = (byte) 0x07;
    static byte GPRS_COMMAND_DATACOMPACT = (byte) 0x35;
    static byte GPRS_COMMAND_DATAFULL = (byte) 0x36;
    static byte GPRS_COMMAND_DATART = (byte) 0x37;
    static byte GPRS_COMMAND_GETCOMMAND = (byte) 0x40;
    static byte GPRS_COMMAND_SENDCOMMAND = (byte) 0x41;
    static byte GPRS_COMMAND_REPLYCOMMAND = (byte) 0x42;
    static byte GPRS_COMMAND_FILEUPLOADSTART = (byte) 0x50;
    static byte GPRS_COMMAND_FILEUPLOADEND = (byte) 0x51;
    static byte GPRS_COMMAND_FILEDOWNLOADSTART = (byte) 0x52;
    static byte GPRS_COMMAND_FILEDOWNLOADEND = (byte) 0x53;
    static byte GPRS_COMMAND_FILECHUNKREQ = (byte) 0x54;
    static byte GPRS_COMMAND_FILECHUNKDATA = (byte) 0x55;
    static byte GPRS_COMMAND_FILEDOWNLOADINFO = (byte) 0x56;
    static byte GPRS_COMMAND_TAXI_SendCommand = (byte) 0x63;
    static byte GPRS_COMMAND_TAXI_ReceiveReply = (byte) 0x64;
    static int SessionCounter = 0;
    static int SessionIDCounter = 0;
    int SessionID = 0;
    DBAdminClass DBAdmin;
    LogsClass Log;
    ConfClass Conf;
    DBConnector DBConn = DBConnector.getInstance();
    Connection DB;

//    static byte [] AESKeyIn=null;
//    static byte [] AESKeyOut=null;
//    static byte [] ZBSN=new byte[4];
//    static int PackN=-1;
    class GPRS_packet {

        byte SessionID[] = new byte[4];
        byte PackN;
        byte Type;
        byte Size;
        byte Crypt;
        byte Spare;
        byte Fill;
        byte Payload[];
    };
    DatagramPacket receivePacket;
    DatagramSocket serverSocket;

    public ZB_UDP_Comunication() {
//        receivePacket=Received;
//        serverSocket=Socket;
    }

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin, ConfClass LConf, LogsClass LLog) {
        DBAdmin = LDBAdmin;
        Conf = LConf;
        Log = LLog;
        return true;
    }

    public boolean SetConnection(DatagramSocket Socket, DatagramPacket Received) {
        receivePacket = Received;
        serverSocket = Socket;
        return true;
    }

    @Override
    public void run() {
        boolean ActiveSession = true;
        boolean InviaNOK = true;
        java.sql.Timestamp TotalStartTime = new java.sql.Timestamp((new java.util.Date()).getTime());
        if (SessionCounter > Conf.ActiveSessionLimit) {
            Log.WriteLog(3, "New connection refused");
            return;
        } else {
            SessionIDCounter++;
            SessionID = SessionIDCounter;
            Log.WriteLog(3, "ID= " + SessionID + "  New connection starting ( opened sessione=" + SessionCounter + ")");
        }
        SessionCounter++;
        try {
            try {
                java.sql.Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
//                float TimeDiff=((float)(now.getTime()-TotalStartTime.getTime()))/1000;
//                Log.WriteLog(3,"ID= "+SessionID+"  pre getConnection time="+TimeDiff);
                DB = DBConn.PoolBB.getConnection();
                DB.setAutoCommit(true);
//                now=new java.sql.Timestamp((new java.util.Date()).getTime());
//                TimeDiff=((float)(now.getTime()-TotalStartTime.getTime()))/1000;
//                Log.WriteLog(3,"ID= "+SessionID+"  getConnection time="+TimeDiff);
            } catch (SQLException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }

            GPRS_packet Pk = new GPRS_packet();

            byte[] sendData = new byte[1024];
            try {
                byte[] data = receivePacket.getData();

                //            Log.WriteLog(3,"Received HEX :"+ (new String(Hex.encode(data))).substring(0,receivePacket.getLength()*2));

                InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();

                if (ReadGPRSPack(Pk, (byte) 0, data) > 0) {
                    //            System.out.println("Ricevuto pacchetto A55A (size="+receivePacket.getLength()+")");

                    zbox ZB = DBAdmin.GetZBox(DB, Pk.SessionID);

                    if (ZB == null) {
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuta connessione da NUOVA ZBox... creazione entry DB");
                        ZB = new zbox();
                        SessionCounter--;
                    }
                    ZB.IP = IPAddress;
                    ZB.Port = port;

                    if (Pk.Type == GPRS_COMMAND_HELO) { // pacchetto di HELO
                        byte[] buff = ReadHelo(ZB, Pk);
                        if (buff != null && SessionCounter < 190) {
                            //sendData=Send_OK(ZB, (byte)0);
                            sendData = SendHelo_Ok(ZB, (byte) 0);
                            DBAdmin.SetZBox(DB, ZB);
                            InviaNOK = false;
                        }
                    } else if (Pk.Type == GPRS_COMMAND_OK) { // pacchetto di OK
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto OK from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        if (ZB.StatoConnessione == 1) { // se mi trovo al termine di un processo di HandShake;
                            ZB.StatoConnessione = 2;
                            DBAdmin.SetZBox(DB, ZB);
                            //                        InviaNOK=false;return;
                        }
                        SessionCounter--;
                        return;
                    } else if (Pk.Type == GPRS_COMMAND_NOK) { // pacchetto di NOK
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto NOK from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        if (ZB.StatoConnessione == 1) { // se mi trovo al termine di un processo di HandShake;
                            ZB.StatoConnessione = 0;
                            DBAdmin.SetZBox(DB, ZB);
                            //                        InviaNOK=false;
                        }
                        SessionCounter--;
                        return;
                    } else if (Pk.Type == GPRS_COMMAND_PUSH_NOTIFY_OK) { // pacchetto NOTIFY_OK
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto PUSH_NOTIFY_OK from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetPushNotifyStatus(ZB.IDBlackBox, 1);
                        SessionCounter--;
                        return;
                    } else if (Pk.Type == GPRS_COMMAND_DATACOMPACT) { // pacchetto dati CompactMode
                        java.sql.Timestamp StartTime = new java.sql.Timestamp((new java.util.Date()).getTime());
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto DATACOMPACT from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        if (ZB.StatoConnessione >= 1) { // se il processo di handshake è correttamente terminato 
                            //                        Log.WriteLog(3,"ID= "+SessionID+"  Payload Decifrato: "+ new String(Hex.encode(Pk.Payload)));
                            int Errors = 0;
//                            if (ZB==null)       // da cancellare appena possibile
                            DBAdmin.SetZBox(DB, ZB);
                            // leggo i record e li archivio nel DB
                            Log.WriteLog(3, "ID= " + SessionID + "  Ricezione corretta archiviazione record su DB");
                            if ((Pk.Size % 3 == 0) && (Pk.Fill == 0)) {
                                int Elem = Pk.Size / 3;
                                for (int i = 0; i < Elem; i++) {
                                    java.sql.Timestamp StartAdd = new java.sql.Timestamp((new java.util.Date()).getTime());
                                    byte Rec[] = new byte[48];
                                    System.arraycopy(Pk.Payload, i * 48, Rec, 0, 48);
                                    ZB_UDP_Record UDP_Rec = DBAdmin.AddRecord(DB, ZB, Rec, true);
                                    if (UDP_Rec.IDRec == 0) {
                                        Errors++;
                                        break;
                                    } else if (UDP_Rec.IDRec > 0) {
                                    }
//                                    java.sql.Timestamp now1=new java.sql.Timestamp((new java.util.Date()).getTime());
//                                    float TimeDiff1=((float)(now1.getTime()-StartTime.getTime()))/1000;
//                                    float TimeDiff2=((float)(now1.getTime()-StartAdd.getTime()))/1000;
//                                    Log.WriteLog(3,"ID= "+SessionID+"  TimeDiff1="+TimeDiff1+"  TimeDiff2="+TimeDiff2);
                                }
                            }

                            // invio OK
                            if (Errors == 0) {
                                sendData = Send_OK(ZB, (byte) 0);
                                InviaNOK = false;
                            }
                        } else {
                            Log.WriteLog(3, "ID= " + SessionID + "  Stato della comunicazione non corretto... dati trascurati");
                        }
                        java.sql.Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
                        float TimeDiff = ((float) (now.getTime() - StartTime.getTime())) / 1000;
                        Log.WriteLog(3, "ID= " + SessionID + "  TimeDiff=" + TimeDiff);

                    } else if (Pk.Type == GPRS_COMMAND_DATART) { // pacchetto dati RealTime
                        ZBRecord[] ZBRec = null;
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto DATART from IP=" + ZB.IP.getHostAddress() + " da ZB : " + new String(Hex.encode(Pk.SessionID)));
                        if (ZB.StatoConnessione >= 1) { // se il processo di handshake è correttamente terminato 
                            Log.WriteLog(3, "Payload Decifrato: " + new String(Hex.encode(Pk.Payload)));
                            int Errors = 0;
//                            if (ZB==null)       // da cancellare appena possibile
                            DBAdmin.SetZBox(DB, ZB);
                            // leggo i record e li archivio nel DB
                            Log.WriteLog(3, "ID= " + SessionID + "  Ricezione corretta archiviazione record su DB");
                            if ((Pk.Size % 3 == 0) && (Pk.Fill == 0)) {
                                int Elem = Pk.Size / 3;
                                ZBRec = new ZBRecord[Elem];
                                for (int i = 0; i < Elem; i++) {
                                    byte Rec[] = new byte[48];
                                    System.arraycopy(Pk.Payload, i * 48, Rec, 0, 48);
                                    ZB_UDP_Record UDP_Rec = DBAdmin.AddRecord(DB, ZB, Rec, true);
                                    java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                                    float TimeDiffRT = ((float) (now1.getTime() - TotalStartTime.getTime())) / 1000;
                                    Log.WriteLog(3, "ID= " + SessionID + " DATART AddRecord  TimeDiffRT=" + TimeDiffRT);
                                    if (UDP_Rec.IDRec == 0) {
                                        Errors++;
                                    } else if (UDP_Rec.IDRec > 0) {
                                        ZBRec[i] = new ZBRecord();
                                        try {
                                            try {
                                                ZBRec[i].ParseRecord(UDP_Rec.IDRec, (long) ZB.IDBlackBox, Rec, new java.sql.Timestamp((new java.util.Date()).getTime()), ZB.IDSWVersion > 17);
                                            } catch (ExceptionInvalidRecordLenght ex) {
                                                DBAdmin.UpdateZBRecordStato(DB, UDP_Rec.IDRec, 51);
                                                continue;
                                            } catch (ExceptionCRCError ex) {
                                                DBAdmin.UpdateZBRecordStato(DB, UDP_Rec.IDRec, 50);
                                                continue;
                                            } catch (Exception ex) {
                                                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                                                continue;
                                            }
                                        } catch (SQLException ex1) {
                                            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex1);
                                        }

                                    }
                                }
                            }

                            // invio OK
                            if (Errors == 0) {
                                sendData = Send_OK(ZB, (byte) 0);
                                if (ZBRec != null) {
                                    try {
                                        DB.setAutoCommit(false);
                                    } catch (SQLException ex) {
                                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                    for (int i = 0; i < ZBRec.length; i++) {
                                        if (ZBRec[i] != null) {
                                            if (ZBRec[i].getRecordType() == ZBRecord.RecordTypes.RecordRT) { //record RealTime
                                                Log.WriteLog(3, "IDZB=" + ZBRec[i].IDBlackBox + " IDRecord =" + ZBRec[i].IDRec + " Rec RT");
                                                try {
//                                                    System.out.println("RealTimeParser Record RT IDRec="+ZBRec[i].IDRec);
                                                    DBAdmin.InsertZBRecord(DB, ZBRec[i]);
                                                } catch (SQLException ex) {
                                                    Log.WriteEx(DBAdminClass.class.getName(), ex);
                                                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                                                }
                                            } else if (ZBRec[i].getRecordType() == ZBRecord.RecordTypes.RecordE) {       //  Record Evento
                                                Log.WriteLog(3, "IDZB=" + ZBRec[i].IDBlackBox + " IDRecord =" + ZBRec[i].IDRec + " Rec E");
                                                try {
                                                    System.out.println("RealTimeParser Record E IDRec=" + ZBRec[i].IDRec);
                                                    DBAdmin.InsertZBRecord(DB, ZBRec[i]);
                                                } catch (SQLException ex) {
                                                    Log.WriteEx(DBAdminClass.class.getName(), ex);
                                                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                                                }
                                            }
                                        }
                                    }
                                    try {
                                        DB.commit();
                                    } catch (SQLException ex) {
                                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                }
                                InviaNOK = false;
                            }
                        } else {
                            Log.WriteLog(3, "ID= " + SessionID + "  Stato della comunicazione non corretto... dati trascurati");
                        }
                    } else if (Pk.Type == GPRS_COMMAND_GETCOMMAND) { // richiesta del comando in coda
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto GETCOMMAND from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        if (ZB.StatoConnessione >= 1) { // se il processo di handshake è correttamente terminato 
                            if (DBAdmin.IsCommandWaiting(ZB.IDBlackBox) > 0) {
                                byte Command[] = DBAdmin.GetCommandWaiting(ZB.IDBlackBox);
                                if (Command != null) {
                                    sendData = Send_Command(ZB, Command);
                                    InviaNOK = false;
                                } else if (Conf.TaxiDataCheck > 0) {
                                    Connection DBTaxi;
                                    try {
                                        DBTaxi = DBConn.PoolTaxiDB.getConnection();
                                        DBTaxi.setAutoCommit(true);
                                        PreparedStatement statement1;
                                        ResultSet rs1;
                                        String QueryString = "SELECT * FROM tozb where stato=0 and IDBlackBox=? and stato=0";
                                        statement1 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                                        statement1.setInt(1, ZB.IDBlackBox);  //IDBlackBox
                                        statement1.setBytes(2, Pk.Payload);
                                        rs1 = statement1.executeQuery();
                                        if (rs1.next()) {
                                            sendData = Send_TAXI_SendCommand(ZB, rs1.getBytes("data"));
                                            InviaNOK = false;
                                        } else {
                                            InviaNOK = true;
                                        }
                                        rs1.close();
                                        statement1.close();
                                    } catch (SQLException ex) {
                                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                }
                            } else {
                                InviaNOK = true;
                            }
                        } else {
                            Log.WriteLog(3, "ID= " + SessionID + "  Stato della comunicazione non corretto... dati trascurati");
                        }
                    } else if (Pk.Type == GPRS_COMMAND_REPLYCOMMAND) { // ricezione risposta da parte della ZB
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto REPLYCOMMAND from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        if (ZB.StatoConnessione >= 1) { // se il processo di handshake è correttamente terminato 
                            if (DBAdmin.IsCommandWaiting(ZB.IDBlackBox) > 0) {
                                boolean ret = DBAdmin.SetCommandWaiting(ZB.IDBlackBox, Pk.Payload);
                                if (ret) {
                                    sendData = Send_OK(ZB, (byte) 0);
                                    InviaNOK = false;
                                } else {
                                    InviaNOK = true;
                                }
                            } else {
                                InviaNOK = true;
                            }
                        } else {
                            Log.WriteLog(3, "ID= " + SessionID + " SID= " + SessionID + "  tato della comunicazione non corretto... dati trascurati");
                        }
                    } else if (Pk.Type == GPRS_COMMAND_FILEUPLOADSTART) { // pacchetto upload start
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEUPLOADSTART da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        long IdFile = ReadFileUploadStart(ZB, Pk);
                        DBAdmin.SetZBox(DB, ZB);
                        if (IdFile != -1) {
                            sendData = Send_FileChunkReq(ZB, IdFile, 0, (byte) 0);
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                    } else if (Pk.Type == GPRS_COMMAND_FILECHUNKDATA) { // pacchetto file chunk data
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILECHUNKDATA da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        FileTrasfer FT = ReadFileChunkData(ZB, Pk);
                        //                    int ChunkNum=DBAdmin.ReadChunkNumFromZBFileUpload(ZB, IdFile)+1;
                        //                    int ChunkTot=DBAdmin.ReadChunkTotFromZBFileUpload(ZB, IdFile);              
                        if (FT.ChunkTot == 1) {
                            sendData = Send_FileUploadEnd(ZB, FT.IDFile, FT.ChunkTot, (byte) 0);
                            InviaNOK = false;
                        } else {
                            if ((FT.ChunkCurr + 1) < FT.ChunkTot) {
                                sendData = Send_FileChunkReq(ZB, FT.IDFile, FT.ChunkCurr + 1, (byte) 0);
                                InviaNOK = false;
                            } else {
                                sendData = Send_FileUploadEnd(ZB, FT.IDFile, FT.ChunkTot, (byte) 0);
                                InviaNOK = false;
                            }
                        }
                    } else if (Pk.Type == GPRS_COMMAND_FILEDOWNLOADSTART) { // pacchetto download start
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADSTART da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        sendData = Send_FileDownloadInfo(ZB);
                        InviaNOK = false;
                    } else if (Pk.Type == GPRS_COMMAND_FILECHUNKREQ) { // pacchetto richiesta di un chunk
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILECHUNKREQ da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        FileTrasfer FT = ReadFileChunkReq(ZB, Pk);
                        sendData = Send_FileChunkData(ZB, FT);
                        if (sendData != null) {
                            DBAdmin.UpdateDataToZBFileDownloadSession(ZB, FT.ChunkCurr, FT.IDFile, 0);
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                    } else if (Pk.Type == GPRS_COMMAND_FILEDOWNLOADEND) { // pacchetto per la fine del download di un file
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADEND da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        DownloadEnd DWEnd = ReadFileDownloadEnd(ZB, Pk);
                        if ((DWEnd.ErrorCode == 0) && (DBAdmin.VerifyHashDownloadFile(ZB, DWEnd.IdFile, DWEnd.Hash) == true)) {
                            DBAdmin.FinalUpdateDataToZBFileDownloadSession(ZB, DWEnd.IdFile, 1);
                            sendData = Send_OK(ZB, (byte) 0);
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                    } else if (Pk.Type == GPRS_COMMAND_TAXI_ReceiveReply && (Conf.TaxiDataCheck > 0)) { // pacchetto richiesta comunicazione verso piatt. Taxi
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto GPRS_COMMAND_TAXI_ReceiveReply da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        Log.WriteLog(3, "ID= " + SessionID + "ZB :" + new String(Hex.encode(Pk.SessionID)) + " data:" + new String(Hex.encode(Pk.Payload)));
                        Connection DBTaxi;
                        try {
                            DBTaxi = DBConn.PoolTaxiDB.getConnection();
                            DBTaxi.setAutoCommit(true);
                            PreparedStatement statement1, statement2;
                            ResultSet rs1;
                            String QueryString = "SELECT * FROM tozb where stato=0 and IDBlackBox=? and stato=0 and Data=?";
                            statement1 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                            statement1.setInt(1, ZB.IDBlackBox);  //IDBlackBox
                            statement1.setBytes(2, Pk.Payload);
                            rs1 = statement1.executeQuery();
                            if (rs1.next()) {
                                java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                                QueryString = "UPDATE tozb SET stato=1, TransmitTimeStamp=? where IDBlackBox=? and data=?";
                                statement2 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                                statement2.setTimestamp(1, now1);
                                statement2.setInt(2, ZB.IDBlackBox);  //IDBlackBox
                                statement2.setBytes(3, Pk.Payload);

                                statement2.execute();

                                sendData = Send_OK(ZB, (byte) 0);
                                InviaNOK = false;

                            } else {
                                java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                                QueryString = "INSERT INTO fromzb (IDBlackBox,Data,ReceiveTimeStamp,stato) VALUES (?, ?, ?, 0)";
                                statement2 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                                statement2.setInt(1, ZB.IDBlackBox);  //IDBlackBox
                                statement2.setBytes(2, Pk.Payload);
                                statement2.setTimestamp(3, now1);

                                statement2.execute();

                                sendData = Send_OK(ZB, (byte) 0);
                                InviaNOK = false;

                            }
                            rs1.close();
                            statement1.close();
                            statement2.close();
                        } catch (SQLException ex) {
                            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto messaggio non riconosciuto (type " + Pk.Type + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        InviaNOK = true;
                    }

                    if (InviaNOK) {
                        Log.WriteLog(3, "ID= " + SessionID + "  Comunicazione non valida -> Invio Nok");
                        sendData = Send_NOK(ZB, (byte) 0);
                    }

                    DatagramPacket sendPacket =
                            new DatagramPacket(sendData, sendData.length, IPAddress, port);
                    serverSocket.send(sendPacket);

                } else {
                    // nel caso di una comunicazione non A55A
                    String sentence = new String(data);
                    sentence = sentence.substring(0, receivePacket.getLength());
                    Log.WriteLog(3, "ID= " + SessionID + "  RECEIVED from IP=" + IPAddress.getHostAddress() + " (size=" + receivePacket.getLength() + "): " + sentence);

                    String capitalizedSentence = sentence.toUpperCase();
                    sendData = capitalizedSentence.getBytes();
                    Log.WriteLog(3, "ID= " + SessionID + "  Received HEX :" + (new String(Hex.encode(receivePacket.getData()))).substring(0, receivePacket.getLength() * 2));
                    if (!capitalizedSentence.startsWith("AT+")) {
                        DatagramPacket sendPacket =
                                new DatagramPacket(sendData, sendData.length, IPAddress, port);
                        serverSocket.send(sendPacket);
                    }
                }

            } catch (IOException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, "ID= " + SessionID + " ", ex);
            }

            java.sql.Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
            float TotalTimeDiff = ((float) (now.getTime() - TotalStartTime.getTime())) / 1000;
            Log.WriteLog(3, "ID= " + SessionID + "  New connection end Type=" + Pk.Type + " ( opened sessione=" + SessionCounter + ") durata=" + TotalTimeDiff);
            SessionCounter--;
            ActiveSession = false;
        } finally {
            if (DB != null) {
                try {
                    DB.close();
                } catch (SQLException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (ActiveSession == true) {
                Log.WriteLog(1, "ID= " + SessionID + "  Errore : sessione non chiusa correttamente");
            }
        }
    }

    /**
     * Legge un packet dal buffer di trasmissione GPRS e ne verifica la
     * validità. Se il pacchetto è valido ritorna 1 e popola la struttura Pk con
     * i dati del pacchetto. Altrimenti ritorna 0
     *
     * @return: 0 pacchetto non valido 1 pacchetto valido e copiato in Pk
     */
    int ReadGPRSPack(GPRS_packet Pk, byte ServerNum, byte data[]) {
        try {
            int i = 0;
            // verifica SOM
//            System.out.println("Verifica SOM");
            if (data[i++] != (byte) 0xa5) {
                return 0;
            }
            if (data[i++] != (byte) 0x5a) {
                return 0;
            }

//            System.out.println("Identificato SOM");
            System.arraycopy(data, i, Pk.SessionID, 0, 4);
            i += 4;
            Pk.PackN = data[i++];
            Pk.Type = data[i++];
            Pk.Size = data[i++];
            Pk.Crypt = (byte) ((data[i] >> 7) & 0x1);
            Pk.Fill = (byte) (data[i++] & 0xF);
            Pk.Payload = new byte[Pk.Size * 16];
            System.arraycopy(data, i, Pk.Payload, 0, Pk.Size * 16);
        } catch (ArrayIndexOutOfBoundsException Ex) {
            Log.WriteLog(2, "ID= " + SessionID + " ReadGPRSPack: errore di interpretazione del pacchetto : " + (new String(Hex.encode(data))));
        }

        int nByte = Pk.Size * 16 + 10;
        if (nByte > data.length + 10) {
            Log.WriteLog(2, "ID= " + SessionID + " Packet size non valido Pk.Size=" + (nByte));
        }

//        Log.WriteLog(3,"Verifica CRC per byte="+(nByte));
        // verifica del crc
        MessageDigest cript;

        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(data, 0, nByte);
        } catch (NoSuchAlgorithmException ex) {
            Log.WriteEx(ZB_UDP_Comunication.class.getName(), ex);
            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }

        byte[] CRC = cript.digest();
        String hexString = (new String(Hex.encode(data))).substring(0, (nByte + 3) * 2);
//        Log.WriteLog(3,"ID= "+SessionID+" ReadGPRSPack Data:"+hexString);

//        hexString = new String(Hex.encode(CRC));
//        System.out.println("CRC :"+hexString);

        if (!(CRC[0] == data[nByte + 0]
                && CRC[1] == data[nByte + 1]
                && CRC[2] == data[nByte + 2])) {
            Log.WriteLog(1, "ID= " + SessionID + " CRC non verificato");
            return 0;
        }

        // se il pacchetto è cifrato provvedo a decifrarlo
        if (Pk.Crypt > 0) {
            zbox ZB = DBAdmin.GetZBox(DB, Pk.SessionID);
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
//            byte[] keyBytes = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
//            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17 };

            SecretKeySpec key = new SecretKeySpec(ZB.AESKeyOut, "AES");
            try {
                Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
                try {
                    cipher.init(Cipher.DECRYPT_MODE, key);
                    Pk.Payload = cipher.doFinal(Pk.Payload);
                } catch (IllegalBlockSizeException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                } catch (BadPaddingException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InvalidKeyException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (NoSuchAlgorithmException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchProviderException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchPaddingException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (Pk.SessionID[0] == 0 && Pk.SessionID[1] == 0 && Pk.SessionID[2] == 0 && Pk.SessionID[3] == 0) { // Seriale di ZB non valido
            return 0;
        }
        return 1;
    }

    byte[] ReadHelo(zbox ZB, GPRS_packet Pk) {
        byte NRootKey = Pk.Payload[0];
        byte VProt = Pk.Payload[1];
        ZB.ProtV = (int) VProt;

        byte[] buff = null;
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        byte[] keyBytes = ZB.AESRootKey[NRootKey];

        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
            try {
                cipher.init(Cipher.DECRYPT_MODE, key);
                buff = cipher.doFinal(Pk.Payload, 16, 32);
                Log.WriteLog(3, "ID= " + SessionID + " ReadHelo from IP=" + ZB.IP.getHostAddress() + " buff size=" + buff.length + ":" + (new String(Hex.encode(buff))));

                System.arraycopy(buff, 0, ZB.AESKeyIn, 0, 16);
                System.arraycopy(buff, 16, ZB.AESKeyOut, 0, 16);

                buff = new byte[16];

                buff[3] = Pk.PackN;
//                PackN=Pk.PackN;
                ZB.PackNRef = Pk.PackN;
                buff[4] = Pk.SessionID[0];
                buff[5] = Pk.SessionID[1];
                buff[6] = Pk.SessionID[2];
                buff[7] = Pk.SessionID[3];
                System.arraycopy(Pk.SessionID, 0, ZB.SerialN, 0, 4);

                Log.WriteLog(3, "ID= " + SessionID + " Ricevuto HELO da ZB :" + new String(Hex.encode(Pk.SessionID)));
            } catch (IllegalBlockSizeException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (BadPaddingException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidKeyException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchProviderException ex) {
            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchPaddingException ex) {
            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
        }

        return buff;
    }

    byte[] SendHelo_Ok(zbox ZB, byte ServerNum) {
        byte[] buff = new byte[16];
        buff[3] = (byte) ZB.PackNRef;
        System.arraycopy(ZB.SerialN, 0, buff, 4, 4);
        Log.WriteLog(3, "ID= " + SessionID + " Invio HELO_Ok da ZB :" + new String(Hex.encode(ZB.SerialN)));

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_HELO_OK, buff, (byte) 0x01);

        if (ret != null) {
            ZB.StatoConnessione = 1;
        } else {
            ZB.StatoConnessione = 0;
        }

        return ret;
    }

    byte[] Send_OK(zbox ZB, byte ServerNum) {
        byte[] buff = new byte[16];
        buff[0] = (byte) ZB.PackNRef;
        System.arraycopy(ZB.SerialN, 0, buff, 2, 4);
        byte Count = DBAdmin.IsCommandWaiting(ZB.IDBlackBox);
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_Ok a ZB :" + new String(Hex.encode(ZB.SerialN)) + " NCommand=" + Count);
        if (Count > 0) {
            buff[1] = Count;
        }

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_OK, buff, (byte) 0x01);

        return ret;
    }

    byte[] Send_NOK(zbox ZB, byte ServerNum) {
        byte[] buff = new byte[16];
        buff[0] = (byte) ZB.PackNRef;
        byte Count = DBAdmin.IsCommandWaiting(ZB.IDBlackBox);
        if (Count > 0) {
            buff[2] = Count;
        }
        System.arraycopy(ZB.SerialN, 0, buff, 3, 4);
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_NOk da ZB :" + new String(Hex.encode(ZB.SerialN)));

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_NOK, buff, (byte) 0x01);

        /*    if (ret!=null) {
         ZB.StatoConnessione=1;
         } else {
         ZB.StatoConnessione=0;
         }*/

        return ret;
    }

    byte[] Send_FileChunkReq(zbox ZB, long IdFile, int ChunkNum, byte ServerNum) {
        byte[] buff = new byte[16];

        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) ((IdFile >> 16) & 0xFF);
        buff[2] = (byte) ((IdFile >> 8) & 0xFF);
        buff[3] = (byte) (IdFile & 0xFF);
        buff[4] = (byte) ((ChunkNum >> 8) & 0xFF);
        buff[5] = (byte) (ChunkNum & 0xFF);
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_FILECHUNKREQ a ZB :" + new String(Hex.encode(ZB.SerialN)));

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_FILECHUNKREQ, buff, (byte) 0x01);

        return ret;
    }

    byte[] Send_FileUploadEnd(zbox ZB, long IdFile, int ChunkTot, byte ServerNum) {
        byte[] buff = new byte[16];
        int ErrorCode = 50;

        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) ((IdFile >> 16) & 0xFF);
        buff[2] = (byte) ((IdFile >> 8) & 0xFF);
        buff[3] = (byte) (IdFile & 0xFF);
        buff[4] = (byte) ((ChunkTot >> 8) & 0xFF);
        buff[5] = (byte) (ChunkTot & 0xFF);
        if (DBAdmin.VerifyHashUploadFile(ZB, IdFile) == true) {
            DBAdmin.UpdateStatoIntoZBFileUpload(ZB, IdFile);
            ErrorCode = 0;
        }
        buff[6] = (byte) ErrorCode;
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND a ZB :" + new String(Hex.encode(ZB.SerialN)));

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_FILEUPLOADEND, buff, (byte) 0x01);

        return ret;
    }

    byte[] Send_FileDownloadInfo(zbox ZB) {
        byte[] buff = new byte[32];
//        byte [] filename=new byte[24];
        int i;

        DownloadInfo DwnlInfo;
        DwnlInfo = DBAdmin.ReadDownloadInfo(ZB);
        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) (DwnlInfo.FileType & 0xFF);
        buff[2] = (byte) ((DwnlInfo.FileSize >> 16) & 0xFF);
        buff[3] = (byte) ((DwnlInfo.FileSize >> 8) & 0xFF);
        buff[4] = (byte) (DwnlInfo.FileSize & 0xFF);
//        filename=DwnlInfo.PathFileName.getBytes();
        for (i = 0; i < Math.min(DwnlInfo.PathFileName.length(), 24); i++) {
            buff[5 + i] = (byte) DwnlInfo.PathFileName.charAt(i);
        }
//        System.arraycopy(filename, 0, buff, 5, DwnlInfo.PathFileName.length());
        buff[29] = (byte) ((DwnlInfo.IDFile >> 16) & 0xFF);
        buff[30] = (byte) ((DwnlInfo.IDFile >> 8) & 0xFF);
        buff[31] = (byte) (DwnlInfo.IDFile & 0xFF);

        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_FILEDOWNLOADINFO, buff, (byte) 0x01);
        return ret;
    }

    /*
     * Invio ChunkData alla ZBOX (Pacchetto FileChuckData Paragrafo 9.3.1.16)
     * @param ZB
     * @param FT
     * @return byte[]
     */
    byte[] Send_FileChunkData(zbox ZB, FileTrasfer FT) {
        byte[] buff = new byte[1008];
        int i;

        DownloadChunkData DwnlChkData;
        DwnlChkData = DBAdmin.ReadDownloadChunkData(ZB, FT);
        if (DwnlChkData == null) {
            return null;
        }
        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) ((DwnlChkData.ByteCount >> 8) & 0xFF);
        buff[2] = (byte) (DwnlChkData.ByteCount & 0xFF);
        buff[3] = (byte) ((DwnlChkData.ChunkNum >> 8) & 0xFF);
        buff[4] = (byte) (DwnlChkData.ChunkNum & 0xFF);
        buff[5] = (byte) ((DwnlChkData.IDFile >> 16) & 0xFF);
        buff[6] = (byte) ((DwnlChkData.IDFile >> 8) & 0xFF);
        buff[7] = (byte) (DwnlChkData.IDFile & 0xFF);
        for (i = 0; i < DwnlChkData.ByteCount; i++) {
            buff[8 + i] = DwnlChkData.ChunkData[i];
        }
        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_FILECHUNKDATA, buff, (byte) 0x01);
        return ret;
    }

    byte[] Send_Command(zbox ZB, byte Command[]) {
        byte Size = (byte) ((Command.length + 2) >> 4);
        byte Resto = (byte) ((Command.length + 2) & 0xF);
        if (Resto != 0) {
            Size++;
        }
        int fill = (byte) ((Size << 4) - (Command.length + 2));
        byte buff[] = new byte[Command.length + fill + 2];

        buff[0] = (byte) ZB.PackNRef;
        System.arraycopy(Command, 0, buff, 1, Command.length);
        // TODO calcolare CRC
        String cmd = new String(Hex.encode(Command));
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_SENDCOMMAND a ZB :" + new String(Hex.encode(ZB.SerialN)) + " Command:" + cmd);

        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_SENDCOMMAND, buff, (byte) 0x01);

        return ret;
    }

    byte[] Send_TAXI_SendCommand(zbox ZB, byte data[]) {
        byte Size = (byte) ((data.length + 2) >> 4);
        byte Resto = (byte) ((data.length + 2) & 0xF);
        if (Resto != 0) {
            Size++;
        }
        int fill = (byte) ((Size << 4) - (data.length + 2));
        byte buff[] = new byte[data.length + fill + 2];

        buff[0] = (byte) ZB.PackNRef;
        System.arraycopy(data, 0, buff, 1, data.length);
        // TODO calcolare CRC
        String cmd = new String(Hex.encode(data));
        Log.WriteLog(3, "ID= " + SessionID + " Invio GPRS_COMMAND_TAXI_SendCommand a ZB :" + new String(Hex.encode(ZB.SerialN)) + " data:" + cmd);

        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_TAXI_SendCommand, buff, (byte) 0x01);

        return ret;
    }

    /**
     * Invia un comando in GPRS alla ZB
     *
     * @param ZB ZBox a cui inviare i dati
     * @param ServerNum ---- non usato
     * @param type Tipo di pacchetto da inviare
     * @param data payload
     * @param Crypt 1 se da cifrare, 0 se da inviare in chiaro
     * @return
     */
    byte[] SendGPRSData(zbox ZB, byte ServerNum, byte type, byte[] data, byte Crypt) {
//        byte [] buff=new byte[data.length+13];
//	DPRINT("PollGPRS SendGPRSData PayloadSize=%d\n\r",data.length);
        int i = 0;
        byte Size = (byte) (data.length >> 4);
        byte Fill;
        byte Resto = (byte) (data.length & 0xF);
        if (Resto != 0) {
            Size++;
        }
        Fill = (byte) ((Size << 4) - data.length);
        byte[] buff = new byte[data.length + 13 + Fill];
        System.out.println("ID= " + SessionID + " SendGPRSData Size=" + Size + " Fill=" + Fill + "Buff.lenght=" + buff.length);

        buff[i] = (byte) 0xA5;
        i++;								// SOM 1
        buff[i] = (byte) 0x5A;
        i++;								// SOM 2

        System.arraycopy(ZB.SerialN, 0, buff, 2, 4);

        i += 4;
        ZB.PackNRef++;
        buff[i] = (byte) ZB.PackNRef;
        i++;                                                  // PackN
        buff[i] = type;
        i++;								// Type
        buff[i] = Size;
        i++;								// size
        buff[i] = (byte) ((Crypt << 7) + Fill);
        i++;            				// Crypt/Fill
        if (Crypt > 0) {
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

            SecretKeySpec key = new SecretKeySpec(ZB.AESKeyIn, "AES");
            try {
                Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
                try {
                    byte[] b;
                    cipher.init(Cipher.ENCRYPT_MODE, key);
                    b = cipher.doFinal(data);
//                    System.out.println("SendGPRSData data.lenght="+data.length+":"+(new String(Hex.encode(data))));
//                    System.out.println("SendGPRSData buff.lenght="+buff.length+":"+(new String(Hex.encode(buff))));
//                    System.out.println("SendGPRSData b.lenght="+b.length+":"+(new String(Hex.encode(b))));
                    System.arraycopy(b, 0, buff, 10, b.length);
                } catch (IllegalBlockSizeException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                } catch (BadPaddingException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InvalidKeyException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (NoSuchAlgorithmException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchProviderException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchPaddingException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {
            System.arraycopy(data, 0, buff, i, data.length);        			// Payload
        }
        i += data.length;
        i += Fill;

        int nByte = i;
        Log.WriteLog(3, "ID= " + SessionID + " SendGPRSData nByte=" + nByte + " i=" + i);

        MessageDigest cript;

        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(buff, 0, nByte);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        byte[] CRC = cript.digest();

        buff[i] = CRC[0];
        i++;
        buff[i] = CRC[1];
        i++;
        buff[i] = CRC[2];
        i++;
        Log.WriteLog(3, "ID= " + SessionID + " SendGPRSData data= " + (new String(Hex.encode(buff))));
        return buff;
    }

    long ReadFileUploadStart(zbox ZB, GPRS_packet Pk) {
        String filename = "";
        byte[] CRC = new byte[20];
        int i;
        int FileType = Utils.uBToI(Pk.Payload[1]);
        int ChunkTot = (Utils.uBToI(Pk.Payload[2]) << 8) + Utils.uBToI(Pk.Payload[3]);
        for (i = 0; i < (Pk.Payload.length - 24); i++) {
            filename += (char) Pk.Payload[4 + i];
        }
        for (i = 0; i < 20; i++) {
            CRC[i] = Pk.Payload[(Pk.Payload.length - 20) + i];
        }

        Log.WriteLog(3, "ID= " + SessionID + " ChunkTot:" + ChunkTot + " filename=" + filename);
        // query insert su zbfileupload
        return DBAdmin.AddStartToZBFileUpload(ZB, FileType, ChunkTot,1000, filename, CRC);
    }

    FileTrasfer ReadFileChunkReq(zbox ZB, GPRS_packet Pk) {
        FileTrasfer FT = new FileTrasfer();

        FT.ChunkCurr = (Utils.uBToI(Pk.Payload[4]) << 8) + Utils.uBToI(Pk.Payload[5]);
        FT.IDFile = (Utils.uBToL(Pk.Payload[1]) << 16) + (Utils.uBToL(Pk.Payload[2]) << 8) + Utils.uBToL(Pk.Payload[3]);
        Log.WriteLog(3, "ID= " + SessionID + " Chunk corrente:" + FT.ChunkCurr + " IdFile=" + FT.IDFile);
        return FT;
    }

    DownloadEnd ReadFileDownloadEnd(zbox ZB, GPRS_packet Pk) {
        DownloadEnd DWEnd = new DownloadEnd();
        int i;

        DWEnd.IdFile = (Utils.uBToL(Pk.Payload[1]) << 16) + (Utils.uBToL(Pk.Payload[2]) << 8) + Utils.uBToL(Pk.Payload[3]);
        DWEnd.ErrorCode = Pk.Payload[7];
        for (i = 0; i < 20; i++) {
            DWEnd.Hash[i] = Pk.Payload[12 + i];
        }
        return DWEnd;
    }

    FileTrasfer ReadFileChunkData(zbox ZB, GPRS_packet Pk) {
        byte[] chunk = new byte[1000];
        int i;
        int ByteCount = (Utils.uBToI(Pk.Payload[1]) << 8) + Utils.uBToI(Pk.Payload[2]);
        int ChunkNum = (Utils.uBToI(Pk.Payload[3]) << 8) + Utils.uBToI(Pk.Payload[4]);
        long IdFile = (Utils.uBToL(Pk.Payload[5]) << 16) + (Utils.uBToL(Pk.Payload[6]) << 8) + Utils.uBToL(Pk.Payload[7]);
        Log.WriteLog(3, "ID= " + SessionID + " ByteCount:" + ByteCount + " ChunkNum=" + ChunkNum + " IdFile=" + IdFile);
        // for (i=1;i<8;i++)
        //    Log.WriteLog(3,"Pk.Payload:["+i+"]"+Pk.Payload[i]);
        for (i = 0; i < ByteCount; i++) {
            chunk[i] = Pk.Payload[8 + i];
        }
//        System.arraycopy(i, i, i, i, i);
        // query update su zbfileupload
        FileTrasfer FT = DBAdmin.UpdateChunkDataToZBFileUpload(ZB, ByteCount, ChunkNum, IdFile, chunk);
        FT.ChunkCurr = ChunkNum;
        return FT;
    }
}
