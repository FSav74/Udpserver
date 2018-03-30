/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.*;
import com.viacom.zbox.execeptions.ExceptionCRCError;
import com.viacom.zbox.execeptions.ExceptionInvalidRecordLenght;
import it.gruppoac.udpserver.kafkaproducer.KafkaProducerPool;
import it.gruppoac.udpserver.kafkaproducer.SimpleProducer;
import it.gruppoac.udpserver.kafkaproducer.exception.ProducerException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.security.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
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
public class ZB_UDP_ComunicationNew implements Runnable{//extends Thread {

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
    static byte GPRS_COMMAND_FILECHUNKREQ = (byte) 0x54;       // type 84
    static byte GPRS_COMMAND_FILECHUNKDATA = (byte) 0x55;       // type 85
    static byte GPRS_COMMAND_FILEDOWNLOADINFO = (byte) 0x56;       // type 86
    static byte GPRS_COMMAND_TAXI_SendCommand = (byte) 0x63;
    static byte GPRS_COMMAND_TAXI_ReceiveReply = (byte) 0x64;
    static byte GPRS_COMMAND_ENABLECODEVERIFY = (byte) 0x70;       // type 112
    static byte GPRS_COMMAND_ENABLECODEREPLY = (byte) 0x71;       // type 113
    static byte GPRS_COMMAND_ENABLELONGCODEVERIFY = (byte) 0x72;       // type 114
    static byte GPRS_COMMAND_ENABLELONGCODEREPLY = (byte) 0x73;       // type 115
    static byte GPRS_COMMAND_FILEUPLOADSTART_ABOX   = (byte) 0x80;
    static byte GPRS_COMMAND_FILEUPLOADEND_ABOX     = (byte) 0x81;
    static byte GPRS_COMMAND_FILEDOWNLOADSTART_ABOX = (byte) 0x82;
    static byte GPRS_COMMAND_FILEDOWNLOADEND_ABOX   = (byte) 0x83;
    static byte GPRS_COMMAND_FILECHUNKREQ_ABOX      = (byte) 0x84;       // type 
    static byte GPRS_COMMAND_FILECHUNKDATA_ABOX     = (byte) 0x85;       // type 
    static byte GPRS_COMMAND_FILEDOWNLOADINFO_ABOX  = (byte) 0x86;       // type 
    //static int SessionCounter = 0;
    static int SessionIDCounter = 0;
    
    private static AtomicLong SessionCounter = new AtomicLong(0);
    
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

    public ZB_UDP_ComunicationNew() {
//        receivePacket=Received;
//        serverSocket=Socket;

        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
    }
    
    //NUovo costruttore
    public ZB_UDP_ComunicationNew(DBAdminClass LDBAdmin, DatagramSocket Socket, DatagramPacket Received) {
//        receivePacket=Received;
//        serverSocket=Socket;

        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
        SetEnvironmentClasses(LDBAdmin);
        SetConnection(Socket, Received);
    }

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin, ConfClass LConf, LogsClass LLog) {
        DBAdmin = LDBAdmin;
        Conf = LConf;
        Log = LLog;
        return true;
    }

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin) {
        DBAdmin = LDBAdmin;

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
        
        //-------------------------------------------------------------------------------------
        //verifica che il contatore è uguale al limite(Conf.ActiveSessionLimit):
        // se è uguale: ritorna true e al contatore assegna di nuovo (Conf.ActiveSessionLimit)
        //-------------------------------------------------------------------------------------
        if (SessionCounter.compareAndSet(Conf.ActiveSessionLimit, Conf.ActiveSessionLimit)){
        //if (SessionCounter > Conf.ActiveSessionLimit) {
            Log.WriteLog(3, "New connection refused");
            return;
        } else {
            SessionIDCounter++;
            SessionID = SessionIDCounter;
            Log.WriteLog(3, "ID= " + SessionID + "  New connection starting ( opened sessione=" + SessionCounter + ")");
        }
        //SessionCounter++;
        SessionCounter.incrementAndGet();
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
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                this.Log.WriteLog(1, "errore recupero connection :"+ex.toString());
                
                //SessionCounter -= 1;
                SessionCounter.decrementAndGet();
                
                if (this.DB != null) {
                    try {
                        this.DB.close();
                    } catch (SQLException ex1) {
                        Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex1);
                        this.Log.WriteLog(1, "errore chiusura connection "+ex1.toString());
                    }
                }
                if (ActiveSession == true) {
                    this.Log.WriteLog(1, "ID= " + this.SessionID + "  Errore : sessione non chiusa correttamente");
                }
                return;
            }catch (Exception ex) {
                Log.WriteLog(1, "Errore recupero connection :"+ex.toString());
                Log.WriteLog(1, "Exception Stacktrace :"+Utils.getStackTrace(ex));
                
                
                
                return;
            }

            GPRS_packet Pk = new GPRS_packet();

            byte[] sendData = null;//new byte[1024];
            try {
                byte[] data = receivePacket.getData();

                //Log.WriteLog(0,"Received HEX :"+ (new String(Hex.encode(data))).substring(0,receivePacket.getLength()*2));

                InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();

                if (ReadGPRSPack(Pk, (byte) 0, data) > 0) {
                    //            System.out.println("Ricevuto pacchetto A55A (size="+receivePacket.getLength()+")");

                    zbox ZB = DBAdmin.GetZBox(DB, Pk.SessionID);

                    if (ZB == null) {
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuta connessione da NUOVA ZBox... creazione entry DB");
                        ZB = new zbox();
                        
                        //SessionCounter--;
                        SessionCounter.decrementAndGet();
                        
                        ActiveSession = false;
                    }
                    ZB.IP = IPAddress;
                    ZB.Port = port;

                    if (Pk.Type == GPRS_COMMAND_HELO) { // pacchetto di HELO
                        byte[] buff = ReadHelo(ZB, Pk);
                        
                        if (buff != null ) {
                        //non lascio questo limite di 190
                        //if (buff != null && SessionCounter < 190) {
                            //sendData=Send_OK(ZB, (byte)0);
                            //System.out.println("ID= " + SessionID + "  SendHelo IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                            Log.WriteLog(1, "ID= " + SessionID + "  SendHelo IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                            sendData = SendHelo_Ok(ZB, (byte) 0);
                            DBAdmin.SetZBox(DB, ZB);
                            InviaNOK = false;
                        }
                    } else if (Pk.Type == GPRS_COMMAND_OK) { // pacchetto di OK
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuto OK from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        if (ZB.StatoConnessione == 1) { // se mi trovo al termine di un processo di HandShake;
                            ZB.StatoConnessione = 2;
                            DBAdmin.SetZBox(DB, ZB);
                        }
                        if (ZB.TKAs_Acknoledge != null) {
                            ZB.TKAs_Acknoledge.SetConnection(DB);
                            ZB.TKAs_Acknoledge.SetReceiveAck();
                        }
                        
                        //SessionCounter--;
                        SessionCounter.decrementAndGet();
                        
                        ActiveSession = false;
                        if (this.DB != null) {
                            try {
                                this.DB.close();
                            } catch (SQLException ex) {
                                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                        if (ActiveSession == true) {
                            this.Log.WriteLog(1, "ID= " + this.SessionID + "  Errore : sessione non chiusa correttamente");
                        }
                        return;
                    } else if (Pk.Type == GPRS_COMMAND_NOK) { // pacchetto di NOK
                        
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuto NOK from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        if (ZB.StatoConnessione == 1) { // se mi trovo al termine di un processo di HandShake;
                            ZB.StatoConnessione = 0;
                            DBAdmin.SetZBox(DB, ZB);
                            //                        InviaNOK=false;
                        }
                        
                        //TODO: il session counter viene decrementato due volte : perchè?
                        
                        //SessionCounter--;
                        ActiveSession = false;
                        //SessionCounter -= 1;
                        SessionCounter.decrementAndGet();//decremento una volta
                        
                        
                        if (this.DB != null) {
                            try {
                                this.DB.close();
                            } catch (SQLException ex) {
                                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                        if (ActiveSession == true) {
                            this.Log.WriteLog(1, "ID= " + this.SessionID + "  Errore : sessione non chiusa correttamente");
                        }

                        return;
                    } else if (Pk.Type == GPRS_COMMAND_PUSH_NOTIFY_OK) { // pacchetto NOTIFY_OK
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuto PUSH_NOTIFY_OK from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetPushNotifyStatus(ZB.IDBlackBox, 1);
                        
                        //TODO: il session counter viene decrementato due volte : perchè?
                        //SessionCounter--;
                        ActiveSession = false;
                        //SessionCounter--;
                        SessionCounter.decrementAndGet();//decremento una volta
                        
                        if (this.DB != null) {
                            try {
                                this.DB.close();
                            } catch (SQLException ex) {
                                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                        if (ActiveSession == true) {
                            this.Log.WriteLog(1, "ID= " + this.SessionID + "  Errore : sessione non chiusa correttamente");
                        }

                        return;
                    } else if (Pk.Type == GPRS_COMMAND_DATACOMPACT) { // pacchetto dati CompactMode
                        java.sql.Timestamp StartTime = new java.sql.Timestamp((new java.util.Date()).getTime());
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuto DATACOMPACT from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
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
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuto DATART from IP=" + ZB.IP.getHostAddress() + " da ZB : " + new String(Hex.encode(Pk.SessionID)));
                        
                        //-----------------------------------------------
                        //  Kafka producer
                        //-----------------------------------------------
                        SimpleProducer simpleProducer = SimpleProducer.INSTANCE;
                        //KafkaProducerPool poolProducer = KafkaProducerPool.INSTANCE;
                        
                        if (ZB.StatoConnessione >= 1) { // se il processo di handshake è correttamente terminato 
                            //Log.WriteLog(3, "Payload Decifrato: " + new String(Hex.encode(Pk.Payload)));
                            int Errors = 0;
                            if (ZB == null) // da cancellare appena possibile
                            {
                                DBAdmin.SetZBox(DB, ZB);
                            }
                            // leggo i record e li archivio nel DB
                            //Log.WriteLog(3, "ID= " + SessionID + "  Ricezione corretta archiviazione record su DB");
                            if ((Pk.Size % 3 == 0) && (Pk.Fill == 0)) {
                                int Elem = Pk.Size / 3;
                                ZBRec = new ZBRecord[Elem];
                                java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                                for (int i = 0; i < Elem; i++) {
                                    byte Rec[] = new byte[48];
                                    System.arraycopy(Pk.Payload, i * 48, Rec, 0, 48);
//                                    ZB_UDP_Record UDP_Rec=DBAdmin.AddRecord(DB,ZB, Rec,true);
                                    
                                    //float TimeDiffRT = ((float) (now1.getTime() - TotalStartTime.getTime())) / 1000;
//                                    Log.WriteLog(3,"ID= "+SessionID+" DATART AddRecord  TimeDiffRT="+TimeDiffRT);
//                                    if (UDP_Rec.IDRec==0)
//                                        Errors++;
//                                    else if (UDP_Rec.IDRec>0) {
                                    ZBRec[i] = new ZBRecord();
//                                        try {

                                    try {
                                        
                                        
                                        ZBRec[i].ParseRecord(0, (long) ZB.IDBlackBox, Rec, new java.sql.Timestamp((new java.util.Date()).getTime()), ZB.IDSWVersion > 17);
                                       
                                        Log.WriteLog(1, ">>IDZB=" + ZBRec[i].IDBlackBox + " IDRecord =" + ZBRec[i].IDRec + " TYPE:"+ZBRec[i].getRecordType());
                                        //-----------------------------------------------
                                        // Call Kafka producer
                                        //-----------------------------------------------
                                        try{
                                            //poolProducer.sendMessage(ZBRec[i]);
                                            simpleProducer.sendSynchronous(ZBRec[i]);
                                        }catch (ProducerException e){
                                            Log.WriteLog(1, ">>Errore ProducerException:"+e.toString());
                                            //---------------------------------------------------
                                            // In caso di errore invio il KO alla zbox
                                            //---------------------------------------------------
                                            Errors++;
                                        }
                                     
                                        /*
                                        try {
                                            if (ZBRec[i].getRecordType() == ZBRecord.RecordTypes.RecordRT) { //record RealTime
                                                Log.WriteLog(3, "IDZB=" + ZBRec[i].IDBlackBox + " IDRecord =" + ZBRec[i].IDRec + " Rec RT");
                                                Log.WriteLog(3, "RealTimeParser Record RT IDRec=" + ZBRec[i].IDRec);
                                                DB.setAutoCommit(true);
                                                DBAdmin.InsertZBRecord(DB, ZBRec[i]);
                                            } else if (ZBRec[i].getRecordType() == ZBRecord.RecordTypes.RecordE) {       //  Record Evento
                                                Log.WriteLog(3, "IDZB=" + ZBRec[i].IDBlackBox + " IDRecord =" + ZBRec[i].IDRec + " Rec E");
                                                Log.WriteLog(3, "RealTimeParser Record E IDRec=" + ZBRec[i].IDRec);
                                                DB.setAutoCommit(true);
                                                DBAdmin.InsertZBRecord(DB, ZBRec[i]);
                                                ManageSpecialEvents(DB,ZB,ZBRec[i]);
                                            } else if (ZBRec[i].getRecordType() == ZBRecord.RecordTypes.RecordI) {        // Record Info
                                                Log.WriteLog(3, "IDZB=" + ZBRec[i].IDBlackBox + " IDRecord =" + ZBRec[i].IDRec + " Rec I");
                                                Log.WriteLog(3, "RealTimeParser Record I IDRec=" + ZBRec[i].IDRec);
                                                DB.setAutoCommit(true);
                                                DBAdmin.InsertZBRecord(DB, ZBRec[i]);
                                            } else {
                                                ZB_UDP_Record UDP_Rec;
                                                if (ZBRec[i].getRecordType() == ZBRecord.RecordTypes.RecordI) {
                                                    UDP_Rec = DBAdmin.AddRecord(DB, ZB, Rec, false);
                                                } else {
                                                    UDP_Rec = DBAdmin.AddRecord(DB, ZB, Rec, true);
                                                }
                                                if (UDP_Rec.IDRec == 0) {
                                                    Log.WriteLog(3, "ID= " + SessionID + " DBAdmin.AddRecord error");
                                                    Errors++;
                                                    break;
                                                }
                                            }
                                        } catch (SQLException ex) {
                                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                                            Errors++;
                                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                                        }
                                        */
                                    } catch (ExceptionInvalidRecordLenght ex) {
//                                                DBAdmin.UpdateZBRecordStato(DB,UDP_Rec.IDRec,51);
                                        Log.WriteLog(3, "ID= " + SessionID + " ExceptionInvalidRecordLenght");
                                        Errors++;
                                        continue;
                                    } catch (ExceptionCRCError ex) {
//                                                DBAdmin.UpdateZBRecordStato(DB,UDP_Rec.IDRec,50);
                                        Log.WriteLog(3, "ID= " + SessionID + " ExceptionCRCError");
                                        // in caso di ricezione di un record con CRC errato lo archivio comunque sulla coda 
                                        // dei record
                                        ZB_UDP_Record UDP_Rec = DBAdmin.AddRecord(DB, ZB, Rec, true);
                                        if (UDP_Rec.IDRec == 0) {
                                            Log.WriteLog(3, "ID= " + SessionID + " DBAdmin.AddRecord error");
                                            Errors++;
                                            break;
                                        }
                                        continue;
                                    } catch (Exception ex) {
                                        Log.WriteLog(3, "ID= " + SessionID + " Exception:" + ex.getMessage());
                                        Log.WriteEx(ZB_UDP_ComunicationNew.class.getName(), ex);
                                        Errors++;
                                        continue;
                                    }

                                }
                                //TEMPI di elaborazione:
                                //float TimeDiffRT = ((float) (now1.getTime() - TotalStartTime.getTime())) / 1000;
                                //Log.WriteLog(1,"ID= "+SessionID+" DATART FINALE  TimeDiffRT="+TimeDiffRT);
                            }
                            //---------------
                            // invio OK
                            //---------------
                            if (Errors == 0) {
                                sendData = Send_OK(ZB, (byte) 0,DB);
                                InviaNOK = false;
                            }
                            //TEMPI di elaborazione::
                            //java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                            //float TimeDiffRT = ((float) (now1.getTime() - TotalStartTime.getTime())) / 1000;
                            //Log.WriteLog(1,"ID= "+SessionID+" DATART FINALE2  TimeDiffRT="+TimeDiffRT);
                        } else {
                            Log.WriteLog(3, "ID= " + SessionID + "  Stato della comunicazione non corretto... dati trascurati");
                        }
                    /* REVISIONE OK */ 
                    } else if (Pk.Type == GPRS_COMMAND_GETCOMMAND) { // richiesta del comando in coda
                        Log.WriteLog(1, "ID= " + SessionID + "  Ricevuto GETCOMMAND from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        if (ZB.StatoConnessione >= 1) { // se il processo di handshake è correttamente terminato 
                            if (DBAdmin.IsCommandWaiting(ZB.IDBlackBox,DB) > 0) {
                                byte Command[] = DBAdmin.GetCommandWaiting(ZB.IDBlackBox,DB);

                                //Log.WriteLog(3,"ID= "+SessionID+"  Conf.TaxiDataCheck="+Conf.TaxiDataCheck);

                                if (Command != null) {
                                    Log.WriteLog(1, "ID= " + SessionID + "  trovato comando Comand size=" + Command.length);
                                    sendData = Send_Command(ZB, Command);
                                    InviaNOK = false;
                                } else if (Conf.TaxiDataCheck > 0) {
                                    Log.WriteLog(3, "ID= " + SessionID + "  Ricerca comando in coda");
                                    Connection DBTaxi;
                                    try {
                                        DBTaxi = DBConn.PoolTaxiDB.getConnection();
                                        DBTaxi.setAutoCommit(true);
                                        PreparedStatement statement1;
                                        ResultSet rs1;
                                        String QueryString = "SELECT * FROM tozb where stato=0 and IDBlackBox=? and stato=0";
                                        Log.WriteLog(3, "ID= " + SessionID + "  richiesta dati tozb");
                                        statement1 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                                        statement1.setInt(1, ZB.IDBlackBox);  //IDBlackBox
                                        rs1 = statement1.executeQuery();
                                        if (rs1.next()) {
                                            DBTaxi = DBConn.PoolTaxiDB.getConnection();
                                            DBTaxi.setAutoCommit(true);

                                            Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " select data:" + new String(Hex.encode(rs1.getBytes("data"))));

                                            sendData = Send_TAXI_SendCommand(ZB, rs1.getBytes("data"));
                                            QueryString = "UPDATE tozb SET RefNum=? where IDToZB=?";
                                            Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " update data:" + new String(Hex.encode(Pk.Payload)));
                                            PreparedStatement statement2 = DBTaxi.prepareStatement(QueryString);
                                            statement2.setInt(1, ZB.PackNRef);
                                            statement2.setLong(2, rs1.getLong("IDToZB"));

                                            statement2.execute();
                                            statement2.close();
                                            InviaNOK = false;
                                        } else {
                                            InviaNOK = true;
                                        }
                                        rs1.close();
                                        statement1.close();
                                    } catch (SQLException ex) {
                                        Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                }
                            } else {
                                Log.WriteLog(1, "ID= " + SessionID + "  Nessun comando trovato per zb " + ZB.IDBlackBox);
                                InviaNOK = true;
                            }
                        } else {
                            Log.WriteLog(3, "ID= " + SessionID + "  Stato della comunicazione non corretto... dati trascurati");
                        }
                    /* - REVISIONE OK - */ 
                    } else if (Pk.Type == GPRS_COMMAND_REPLYCOMMAND) { // ricezione risposta da parte della ZB
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto REPLYCOMMAND from IP=" + ZB.IP.getHostAddress() + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        if (ZB.StatoConnessione >= 1) { // se il processo di handshake è correttamente terminato 
                            if (DBAdmin.IsCommandWaiting(ZB.IDBlackBox, DB) > 0) {
                                boolean ret = DBAdmin.SetCommandWaiting(ZB.IDBlackBox, Pk.Payload, DB);
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
                            Log.WriteLog(3, "ID= " + SessionID + " SID= " + SessionID + "  stato della comunicazione non corretto... dati trascurati");
                        }
                    /* - REVISIONE OK - */
                   } else if (Pk.Type == GPRS_COMMAND_FILEUPLOADSTART) { // pacchetto upload start
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEUPLOADSTART da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        long IdFile = ReadFileUploadStart(ZB, Pk, DB);
                        DBAdmin.SetZBox(DB, ZB);
                        if (IdFile != -1) {
                            sendData = Send_FileChunkReq(ZB, IdFile, 0, (byte) 0);//non
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                   /* - REVISIONE OK - */        
                   } else if (Pk.Type == GPRS_COMMAND_FILECHUNKDATA) { // pacchetto file chunk data
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILECHUNKDATA da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        FileTrasfer FT = ReadFileChunkData(ZB, Pk ,DB);
                         
                        if (FT!=null){
                            if (FT.ChunkTot == 1) {
                                sendData = Send_FileUploadEnd(ZB, FT.IDFile, FT.ChunkTot, (byte) 0, DB);
                                InviaNOK = false;
                            } else {
                                if ((FT.ChunkCurr + 1) < FT.ChunkTot) {
                                    sendData = Send_FileChunkReq(ZB, FT.IDFile, FT.ChunkCurr + 1, (byte) 0 ); //non
                                    InviaNOK = false;
                                } else {
                                    sendData = Send_FileUploadEnd(ZB, FT.IDFile, FT.ChunkTot, (byte) 0, DB);
                                    InviaNOK = false;
                                }
                            }
                        }else{
                            Log.WriteLog(1, "ID= " + SessionID + "  Stato della comunicazione non corretto... COMMAND_FILECHUNKDATA dati trascurati");
                        }
                    /* - REVISIONE OK - */     
                    } else if (Pk.Type == GPRS_COMMAND_FILEUPLOADSTART_ABOX) { // pacchetto upload start
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEUPLOADSTART_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                       
                        //System.out.println("ID= " + SessionID + "  Ricevuto COMMAND_FILEUPLOADSTART_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        long IdFile = ReadFileUploadStart_ABOX(ZB, Pk, DB);
                        DBAdmin.SetZBox(DB, ZB);
                        if (IdFile != -1) {
                            sendData = Send_FileChunkReq_ABOX(ZB, IdFile, 0, (byte) 0);//non
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                    /* - REVISIONE OK - */         
                    } else if (Pk.Type == GPRS_COMMAND_FILECHUNKDATA_ABOX) { // pacchetto file chunk data
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILECHUNKDATA_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        
                        DBAdmin.SetZBox(DB, ZB);
                        FileTrasfer FT = ReadFileChunkData_ABOX(ZB, Pk, DB);
                        //                    int ChunkNum=DBAdmin.ReadChunkNumFromZBFileUpload(ZB, IdFile)+1;
                        //                    int ChunkTot=DBAdmin.ReadChunkTotFromZBFileUpload(ZB, IdFile);
                        if (FT!=null){
                            if (FT.ChunkTot == 1) {
                                sendData = Send_FileUploadEnd_ABOX(ZB, FT.IDFile, FT.ChunkTot, (byte) 0, DB);
                                InviaNOK = false;
                            } else {
                                if ((FT.ChunkCurr + 1) < FT.ChunkTot) {
                                    sendData = Send_FileChunkReq_ABOX(ZB, FT.IDFile, FT.ChunkCurr + 1, (byte) 0);//non
                                    InviaNOK = false;
                                } else {
                                    sendData = Send_FileUploadEnd_ABOX(ZB, FT.IDFile, FT.ChunkTot, (byte) 0, DB);
                                    InviaNOK = false;
                                }
                            }
                        }else{
                            Log.WriteLog(1, "ID= " + SessionID + "  Stato della comunicazione non corretto... COMMAND_FILECHUNKDATA_ABOX dati trascurati");
                        }
                    /* -- REVISIONE OK -*/      
                    } else if (Pk.Type == GPRS_COMMAND_FILEDOWNLOADSTART) { // pacchetto download start
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADSTART da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        try {
                            sendData = Send_FileDownloadInfo(ZB, DB);
                            InviaNOK = false;
                        } catch (Exception ex) {
                            Log.WriteLog(3, "ID= " + SessionID + "  Errore COMMAND_FILEDOWNLOADSTART da ZB :" + new String(Hex.encode(Pk.SessionID)) + 
                                    " Exception:"+ex.toString());
                        }
                    /* - REVISIONE OK -*/       
                    } else if (Pk.Type == GPRS_COMMAND_FILECHUNKREQ) { // pacchetto richiesta di un chunk
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILECHUNKREQ da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        FileTrasfer FT = ReadFileChunkReq(ZB, Pk); //non
                        sendData = Send_FileChunkData(ZB, FT, DB);
                        if (sendData != null) {
                            DBAdmin.UpdateDataToZBFileDownloadSession(ZB, FT.ChunkCurr, FT.IDFile, 0, DB);
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                    /* - REVISIONE OK - */   
                    } else if (Pk.Type == GPRS_COMMAND_FILEDOWNLOADEND) { // pacchetto per la fine del download di un file
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADEND da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        DownloadEnd DWEnd = ReadFileDownloadEnd(ZB, Pk); //NON
                        if ((DWEnd.ErrorCode == 0) && (DBAdmin.VerifyHashDownloadFile(ZB, DWEnd.IdFile, DWEnd.Hash, DB) == true)) {
                            DBAdmin.FinalUpdateDataToZBFileDownloadSession(ZB, DWEnd.IdFile, 1, DB);
                            sendData = Send_OK(ZB, (byte) 0);
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                    /* - REVISIONE OK -*/    
                    } else if (Pk.Type == GPRS_COMMAND_FILEDOWNLOADSTART_ABOX) { // pacchetto download start ABOX
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADSTART_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        System.out.append("ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADSTART_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        try {
                            System.out.println("send GPRS_COMMAND_FILEDOWNLOADINFO_ABOX");
                            sendData = Send_FileDownloadInfo_ABOX(ZB, DB);
                            InviaNOK = false;
                        } catch (Exception ex) {
                            Log.WriteLog(3, "ID= " + SessionID + "  Errore COMMAND_FILEDOWNLOADSTART_ABOX  da ZB :" + new String(Hex.encode(Pk.SessionID))+""
                                    + " Exception:"+ex.toString());
                        }
                     /* - REVISIONE OK - */    
                    } else if (Pk.Type == GPRS_COMMAND_FILECHUNKREQ_ABOX) { // pacchetto richiesta di un chunk ABOX
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILECHUNKREQ_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        System.out.println("ID= " + SessionID + "  Ricevuto COMMAND_FILECHUNKREQ_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        FileTrasfer FT = ReadFileChunkReq_ABOX(ZB, Pk);//NON
                        sendData = Send_FileChunkData_ABOX(ZB, FT, DB);
                        if (sendData != null) {
                            DBAdmin.UpdateDataToZBFileDownloadSession(ZB, FT.ChunkCurr, FT.IDFile, 0, DB);
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                     /* - REVISIONE OK - */    
                    } else if (Pk.Type == GPRS_COMMAND_FILEDOWNLOADEND_ABOX) { // pacchetto ABOX per la fine del download di un file
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADEND_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        System.out.println("ID= " + SessionID + "  Ricevuto COMMAND_FILEDOWNLOADEND_ABOX da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        DBAdmin.SetZBox(DB, ZB);
                        DownloadEnd DWEnd = ReadFileDownloadEnd(ZB, Pk);//NON
                        if ((DWEnd.ErrorCode == 0) && (DBAdmin.VerifyHashDownloadFile(ZB, DWEnd.IdFile, DWEnd.Hash, DB) == true)) {
                            DBAdmin.FinalUpdateDataToZBFileDownloadSession(ZB, DWEnd.IdFile, 1, DB);
                            sendData = Send_OK(ZB, (byte) 0);
                            InviaNOK = false;
                        } else {
                            InviaNOK = true;
                        }
                    /*    */
                    } else if (Pk.Type == GPRS_COMMAND_TAXI_ReceiveReply && (Conf.TaxiDataCheck > 0)) { // pacchetto richiesta comunicazione verso piatt. Taxi
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto GPRS_COMMAND_TAXI_ReceiveReply da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " data:" + new String(Hex.encode(Pk.Payload)));
                        Connection DBTaxi;
                        try {
                            DBTaxi = DBConn.PoolTaxiDB.getConnection();
                            DBTaxi.setAutoCommit(true);
                            PreparedStatement statement1, statement2;
                            ResultSet rs1;
                            byte DataTaxi[] = new byte[Pk.Size * 16 - Pk.Fill - 1];
                            Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " Pk.Size=" + Pk.Size + " Pk.Fill=" + Pk.Fill
                                    + " Payload[0]=" + new String(Hex.encode(Pk.Payload)));
                            System.arraycopy(Pk.Payload, 1, DataTaxi, 0, Pk.Size * 16 - Pk.Fill - 2);
//                            String QueryString = "SELECT * FROM tozb where stato=0 and IDBlackBox=? and stato=0 and Data=?";
                            String QueryString = "SELECT * FROM tozb where stato=0 and IDBlackBox=? and stato=0 and RefNum=?";
                            statement1 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                            statement1.setInt(1, ZB.IDBlackBox);  //IDBlackBox
                            Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " select data:" + new String(Hex.encode(DataTaxi)));
                            statement1.setByte(2, Pk.Payload[0]);
                            rs1 = statement1.executeQuery();
                            if (rs1.next()) {
                                java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                                QueryString = "UPDATE tozb SET stato=1, TransmitTimeStamp=? where IDBlackBox=? and IDToZB=?";
                                Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " update data:" + new String(Hex.encode(Pk.Payload)));
                                statement2 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                                statement2.setTimestamp(1, now1);
                                statement2.setInt(2, ZB.IDBlackBox);  //IDBlackBox
                                statement2.setLong(3, rs1.getLong("IDToZB"));

                                statement2.execute();

                                Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " update OK");

                                sendData = Send_OK(ZB, (byte) 0);
                                InviaNOK = false;

                            } else {
                                java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                                QueryString = "INSERT INTO fromzb (IDBlackBox,Data,ReceiveTimeStamp,stato) VALUES (?, ?, ?, 0)";
                                statement2 = DBTaxi.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                                Log.WriteLog(3, "ID= " + SessionID + " ZB :" + new String(Hex.encode(Pk.SessionID)) + " insert data:" + new String(Hex.encode(Pk.Payload)));
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
                            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else if (Pk.Type == GPRS_COMMAND_ENABLECODEVERIFY) { // pacchetto dati EnableCodeVerify
                        TokenAuths As = Read_GPRS_COMMAND_ENABLECODEVERIFY(ZB, Pk);
                        if (As == null) {
                            InviaNOK = true;
                        } else {
                            sendData = Send_GPRS_COMMAND_ENABLECODEREPLY(ZB, As);
                            InviaNOK = false;
                        }
                    } else if (Pk.Type == GPRS_COMMAND_ENABLELONGCODEVERIFY) {
                        TokenAuths As = Read_GPRS_COMMAND_ENABLELONGCODEVERIFY(ZB, Pk);
                        if (As == null) {
                            InviaNOK = true;
                        } else {
                            sendData = Send_GPRS_COMMAND_ENABLELONGCODEREPLY(ZB, As);
                            InviaNOK = false;
                        }
                    } else {
                        Log.WriteLog(3, "ID= " + SessionID + "  Ricevuto messaggio non riconosciuto (type " + Pk.Type + " da ZB :" + new String(Hex.encode(Pk.SessionID)));
                        InviaNOK = true;
                    }

                    if (InviaNOK) {
                        Log.WriteLog(3, "ID= " + SessionID + "  Comunicazione non valida -> Invio Nok");
                        sendData = Send_NOK(ZB, (byte) 0, DB);
                    }

                    DatagramPacket sendPacket =
                            new DatagramPacket(sendData, sendData.length, IPAddress, port);
                    serverSocket.send(sendPacket);

                } else {
                    // nel caso di una comunicazione non A55A
                    String sentence = new String(data);
                    sentence = sentence.substring(0, receivePacket.getLength());
                    System.out.println(sentence);
                    System.out.println("ID= " + SessionID 
                            + "  Received HEX :" + (new String(Hex.encode(receivePacket.getData()))).substring(0, receivePacket.getLength() * 2)
                            );
                    Log.WriteLog(3, "ID= " + SessionID + "  RECEIVED from IP=" + IPAddress.getHostAddress() + " (size=" + receivePacket.getLength() + "): " + sentence);

                    String capitalizedSentence = sentence.toUpperCase();
                    sendData = capitalizedSentence.getBytes();
                    sendData = "Stringa di risposta\n\r".getBytes();
                    sendData = sentence.getBytes();
                    
                    Log.WriteLog(3, "ID= " + SessionID + "  Received HEX :" + (new String(Hex.encode(receivePacket.getData()))).substring(0, receivePacket.getLength() * 2));
                    if (!capitalizedSentence.startsWith("AT+")) {
                        DatagramPacket sendPacket =
                                new DatagramPacket(sendData, sendData.length, IPAddress, port);
                        serverSocket.send(sendPacket);
                    }
                }

            } catch (IOException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, "ID= " + SessionID + " ", ex);
            }

            java.sql.Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
            float TotalTimeDiff = ((float) (now.getTime() - TotalStartTime.getTime())) / 1000;
            Log.WriteLog(3, "ID= " + SessionID + "  New connection end Type=" + Pk.Type + " ( opened sessione=" + SessionCounter + ") durata=" + TotalTimeDiff);
            //System.out.println("ID= " + SessionID + "  New connection end Type=" + Pk.Type + " ( opened sessione=" + SessionCounter + ") durata=" + TotalTimeDiff);

            ActiveSession = false;
            sendData= null;
        } catch (Exception e){
            Log.WriteLog(1, "Exception end :"+e.getMessage());
            Log.WriteLog(1, "Exception  :"+Utils.getStackTrace(e));
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            
            Log.WriteLog(1, "-Stacktrace :"+sw.toString());
        } catch (Throwable ex2) {
                    
            Log.WriteLog(1, "Exception -5- end : "+ex2.toString());
            Log.WriteLog(1, "Exception Stacktrace :"+Utils.getStackTrace(ex2));   
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex2.printStackTrace(pw);
           
            Log.WriteLog(1, "-Stacktrace :"+sw.toString());
        } finally {
            //SessionCounter--;
            SessionCounter.decrementAndGet();
            if (DB != null) {
                try {
                    DB.close();
                } catch (SQLException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
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
            Log.WriteEx(ZB_UDP_ComunicationNew.class.getName(), ex);
            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
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
            
           //MODIFICA PER TEST CON JMETER
           
            //Log.WriteLog(1, "ID= " + SessionID + " CRC non verificato (type=" + Pk.Type + ")\n\r" + hexString);
            //return 0;
            
          //FIME MODIFICA PER TESTCON JMETER  
        }

        // se il pacchetto è cifrato provvedo a decifrarlo
        if (Pk.Crypt > 0) {
            zbox ZB = DBAdmin.GetZBox(DB, Pk.SessionID);
            
            if (ZB == null){
                // E' arrivato un messaggio criptato ma
                // la zbox non è presente nel db (non è arrivato HELO)
                // la comunicazione non è valida.
                return 0;
            }
                
            
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
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                } catch (BadPaddingException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InvalidKeyException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (NoSuchAlgorithmException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchProviderException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchPaddingException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
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
        if (NRootKey > ZB.AESRootKey.length) {
            Log.WriteLog(1, "Errore ID=" + SessionID + " IDBB=" + ZB.IDBlackBox + " SN=" + Hex.encode(ZB.SerialN) + " NRootKey=" + NRootKey + " non valida");
            return null;
        }
        byte[] keyBytes = ZB.AESRootKey[NRootKey];

        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
            try {
                cipher.init(Cipher.DECRYPT_MODE, key);
                buff = cipher.doFinal(Pk.Payload, 16, 32);
                Log.WriteLog(3, "ID= " + SessionID + " ReadHelo  NRootKey=" + NRootKey + " from IP=" + ZB.IP.getHostAddress() + " buff size=" + buff.length + ":" + (new String(Hex.encode(buff))));
                //System.out.println("ID= " + SessionID + " ReadHelo  NRootKey=" + NRootKey + " from IP=" + ZB.IP.getHostAddress() + " buff size=" + buff.length + ":" + (new String(Hex.encode(buff))));

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
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            } catch (BadPaddingException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidKeyException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchProviderException ex) {
            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchPaddingException ex) {
            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
        }

        return buff;
    }

    byte[] SendHelo_Ok(zbox ZB, byte ServerNum) {
        byte[] buff = new byte[16];
        long unixTime = System.currentTimeMillis() / 1000L;
        buff[0] = (byte) ((unixTime >> 24) & 0xFF);
        buff[1] = (byte) ((unixTime >> 16) & 0xFF);
        buff[2] = (byte) ((unixTime >> 8) & 0xFF);
        buff[3] = (byte) ((unixTime) & 0xFF);
        buff[8] = (byte) ZB.PackNRef;
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
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_Ok a ZB :" + new String(Hex.encode(ZB.SerialN)) + " NCommand=" + Count + " IDBlackBox=" + ZB.IDBlackBox);
        if (Count > 0) {
            buff[1] = Count;
        }

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_OK, buff, (byte) 0x01);

        return ret;
    }
    
    //----------------------------------------------
    //Metodo modificato per portare la connessione
    //----------------------------------------------
     byte[] Send_OK(zbox ZB, byte ServerNum, Connection conn) {
        byte[] buff = new byte[16];
        buff[0] = (byte) ZB.PackNRef;
        System.arraycopy(ZB.SerialN, 0, buff, 2, 4);
        byte Count = DBAdmin.IsCommandWaiting(ZB.IDBlackBox, conn);
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_Ok a ZB :" + new String(Hex.encode(ZB.SerialN)) + " NCommand=" + Count + " IDBlackBox=" + ZB.IDBlackBox);
        if (Count > 0) {
            buff[1] = Count;
        }

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_OK, buff, (byte) 0x01);

        return ret;
    }
    //----------------------------------------------
    //Metodo modificato per portare la connessione
    //----------------------------------------------
     byte[] Send_NOK(zbox ZB, byte ServerNum, Connection conn) {
        byte[] buff = new byte[16];
        buff[0] = (byte) ZB.PackNRef;
        byte Count = DBAdmin.IsCommandWaiting(ZB.IDBlackBox, conn);
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

    
    byte[] Send_FileChunkReq_ABOX(zbox ZB, long IdFile, int ChunkNum, byte ServerNum) {
        byte[] buff = new byte[16];

        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) ((IdFile >> 16) & 0xFF);
        buff[2] = (byte) ((IdFile >> 8) & 0xFF);
        buff[3] = (byte) (IdFile & 0xFF);
        buff[4] = (byte) ((ChunkNum >> 8) & 0xFF);
        buff[5] = (byte) (ChunkNum & 0xFF);
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_FILECHUNKREQ_ABOX a ZB :" + new String(Hex.encode(ZB.SerialN)));

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_FILECHUNKREQ_ABOX, buff, (byte) 0x01);

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
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND a ZB :" + new String(Hex.encode(ZB.SerialN))+ " ErrorCode "+ErrorCode);
        System.out.println( "ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND a ZB :" + new String(Hex.encode(ZB.SerialN))+ " ErrorCode "+ErrorCode);
        

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_FILEUPLOADEND, buff, (byte) 0x01);

        return ret;
    }

    byte[] Send_FileUploadEnd_ABOX(zbox ZB, long IdFile, int ChunkTot, byte ServerNum) {
        byte[] buff = new byte[16];
        int ErrorCode = 50;

        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) ((IdFile >> 16) & 0xFF);
        buff[2] = (byte) ((IdFile >> 8) & 0xFF);
        buff[3] = (byte) (IdFile & 0xFF);
        buff[4] = (byte) ((ChunkTot >> 8) & 0xFF);
        buff[5] = (byte) (ChunkTot & 0xFF);
//        if (DBAdmin.VerifyHashUploadFile(ZB, IdFile) == true) {
            DBAdmin.UpdateStatoIntoZBFileUpload(ZB, IdFile);
            ErrorCode = 0;
//        }
        buff[6] = (byte) ErrorCode;
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND_ABOX a ZB :" + new String(Hex.encode(ZB.SerialN)));
        System.out.println("ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND_ABOX a ZB :" + new String(Hex.encode(ZB.SerialN)));

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_FILEUPLOADEND_ABOX, buff, (byte) 0x01);

        return ret;
    }

    byte[] Send_FileDownloadInfo(zbox ZB) throws Exception {
        byte[] buff = new byte[32];
//        byte [] filename=new byte[24];
        int i;

        DownloadInfo DwnlInfo;
        DwnlInfo = DBAdmin.ReadDownloadInfo(ZB);
        if (DwnlInfo == null) {
            throw new Exception("File non trovato");
        }
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

    byte[] Send_FileDownloadInfo_ABOX(zbox ZB) throws Exception {
        byte[] buff = new byte[32];
//        byte [] filename=new byte[24];
        int i;

        DownloadInfo DwnlInfo;
        DwnlInfo = DBAdmin.ReadDownloadInfo(ZB);
        if (DwnlInfo == null) {
            throw new Exception("File non trovato");
        }
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

        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_FILEDOWNLOADINFO_ABOX, buff, (byte) 0x01);
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

    /*
     * Invio ChunkData alla ZBOX (Pacchetto FileChuckData Paragrafo 9.3.1.16)
     * @param ZB
     * @param FT
     * @return byte[]
     */
    byte[] Send_FileChunkData_ABOX(zbox ZB, FileTrasfer FT) {
        byte[] buff = new byte[FT.ChunkSize+8];
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
        byte[] buff1 = new byte[DwnlChkData.ByteCount+8];
        System.arraycopy(buff, 0, buff1, 0, DwnlChkData.ByteCount+8);
        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_FILECHUNKDATA_ABOX, buff1, (byte) 0x01);
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
//        byte Size=(byte)((data.length+1)>>4);
//        byte Resto=(byte)((data.length+1)&0xF);
//	if (Resto!=0) Size++;
////        int fill=(byte)((Size<<4)-(data.length+2));
//        byte buff[]= new byte[data.length+1];
//
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
//        byte [] buff=new byte[data.length+13+Fill];
        byte[] buff = new byte[(Size * 16) + 13];
        //System.out.println("ID= " + SessionID + " SendGPRSData Size=" + Size + " Fill=" + Fill + " Buff.lenght=" + buff.length);

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
                    byte FillBuffer[] = new byte[Size * 16];
                    //System.out.println("ID= " + SessionID + " SendGPRSData FillBuffer.length=" + FillBuffer.length);
                    System.arraycopy(data, 0, FillBuffer, 0, data.length);
                    cipher.init(Cipher.ENCRYPT_MODE, key);
//                    b=cipher.doFinal(data);
                    b = cipher.doFinal(FillBuffer);
//                    System.out.println("SendGPRSData data.lenght="+data.length+":"+(new String(Hex.encode(data))));
//                    System.out.println("SendGPRSData buff.lenght="+buff.length+":"+(new String(Hex.encode(buff))));
//                    System.out.println("SendGPRSData b.lenght="+b.length+":"+(new String(Hex.encode(b))));
                    System.arraycopy(b, 0, buff, 10, b.length);
                } catch (IllegalBlockSizeException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                } catch (BadPaddingException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InvalidKeyException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (NoSuchAlgorithmException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchProviderException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchPaddingException ex) {
                Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {
            System.arraycopy(data, 0, buff, i, data.length);        			// Payload
        }
//	i+=data.length;
//	i+=Fill;
        i += Size * 16;

        int nByte = i;
        //Log.WriteLog(3, "ID= " + SessionID + " SendGPRSData nByte=" + nByte + " i=" + i);

        MessageDigest cript;

        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(buff, 0, nByte);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        byte[] CRC = cript.digest();

        buff[i] = CRC[0];
        i++;
        buff[i] = CRC[1];
        i++;
        buff[i] = CRC[2];
        i++;
        //Log.WriteLog(3, "ID= " + SessionID + " SendGPRSData data= " + (new String(Hex.encode(buff))));
        return buff;
    }

    TokenAuths Read_GPRS_COMMAND_ENABLECODEVERIFY(zbox ZB, GPRS_packet Pk) {
        //int c=GPRS_COMMAND_ENABLECODEVERIFY;
        byte Type = Pk.Payload[1];
        byte[] TokenHWID = new byte[4];
        System.arraycopy(Pk.Payload, 2, TokenHWID, 0, 4);
        long TokenHWID_N = (Utils.uBToL(TokenHWID[0]) << 24)
                + (Utils.uBToL(TokenHWID[1]) << 16)
                + (Utils.uBToL(TokenHWID[2]) << 8)
                + Utils.uBToL(TokenHWID[3]);
        Token TK = new Token(DB);
        TK.SetTokenHWID_N(TokenHWID_N);
        TK.IDAzienda = ZB.IDAzienda;
        TK.ReadToken();
        TokenAuths TKAs = null;
        Log.WriteLog(3, "TokenHWID_N Read_GPRS_COMMAND_ENABLECODEVERIFY ZB= " + ZB.IDBlackBox + " req Type=" + Type + " IDToken=" + TK.IDToken + " TokenHWID_N=" + TokenHWID_N + " IDAzienda=" + TK.IDAzienda);
        System.out.println("TokenHWID_N Read_GPRS_COMMAND_ENABLECODEVERIFY ZB= " + ZB.IDBlackBox + " req Type=" + Type + " IDToken=" + TK.IDToken + " TokenHWID_N=" + TokenHWID_N + " IDAzienda=" + TK.IDAzienda);

        ZB_ReservationProcess ZB_Reservation = new ZB_ReservationProcess();
        ZB_Reservation.SetEnvironmentClasses(DBAdmin, Conf, Log);
        try {
            ZB_Reservation.CheckReservation_ZB_Stato(ZB.IDBlackBox);
        } catch (SQLException ex) {
            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (TK.IDToken < 0) {
            System.out.println("Token non trovato");
        } else if (Type == 1) {          // richiesta di verifica abilitazione per il singolo codice TokenHWID
            TKAs = new TokenAuths(DB);
            TKAs.ReadAuth(ZB.IDBlackBox, TK.IDToken, TokenHWID_N, true, TK);

        } else if (Type == 2) {   // richiesta di verifica abilitazione/disabilitazione per tutti i codici eventualmente associati
            TKAs = new TokenAuths(DB);
            TK.IDToken = -1;
            TK.TokenHWID_N = -1;
            TKAs.ReadAuth(ZB.IDBlackBox, -1, -1, false, TK);

        } else if (Type == 3) {   // richiesta di ritrasmissione abilitazione/disabilitazione per tutti i codici eventualmente associati 
            TKAs = new TokenAuths(DB);
            TK.IDToken = -1;
            TK.TokenHWID_N = -1;
            TKAs.ReadAuth(ZB.IDBlackBox, -1, -1, true, TK);

        } else if (Type == 4) {  // richiesta di ritrasmissione abilitazione/disabilitazione per uno specifico Token
            TKAs = new TokenAuths(DB);
            TKAs.ReadAuth(ZB.IDBlackBox, TK.IDToken, TokenHWID_N, true, TK);
        }
        return TKAs;
    }

    TokenAuths Read_GPRS_COMMAND_ENABLELONGCODEVERIFY(zbox ZB, GPRS_packet Pk) {
        java.text.SimpleDateFormat FullDateFormat = new java.text.SimpleDateFormat("[dd/MM/yyyy HH:mm:ss.SSSS] ");
        String FullDate = FullDateFormat.format(new Date());

        byte Type = Pk.Payload[1];
        byte[] TokenHWID = new byte[6];
        System.arraycopy(Pk.Payload, 2, TokenHWID, 0, 6);
        long TokenHWID_N = (Utils.uBToL(TokenHWID[0]) << 40)
                + (Utils.uBToL(TokenHWID[1]) << 32)
                + (Utils.uBToL(TokenHWID[2]) << 24)
                + (Utils.uBToL(TokenHWID[3]) << 16)
                + (Utils.uBToL(TokenHWID[4]) << 8)
                + Utils.uBToL(TokenHWID[5]);

        Token TK = new Token(this.DB);
        TK.SetTokenHWID_N(TokenHWID_N);
        TK.IDAzienda = ZB.IDAzienda;
        TK.ReadLongToken();
        TokenAuths TKAs = null;
        this.Log.WriteLog(3, "TokenHWID_N Read_GPRS_COMMAND_ENABLELONGCODEVERIFY ZB= " + ZB.IDBlackBox + " req Type=" + Type + " IDToken=" + TK.IDToken + " TokenHWID_N=" + TokenHWID_N + " IDAzienda=" + TK.IDAzienda);
        System.out.println(FullDate + "TokenHWID_N Read_GPRS_COMMAND_ENABLELONGCODEVERIFY ZB= " + ZB.IDBlackBox + " req Type=" + Type + " IDToken=" + TK.IDToken + " TokenHWID_N=" + TokenHWID_N + " IDAzienda=" + TK.IDAzienda);

        ZB_ReservationProcess ZB_Reservation = new ZB_ReservationProcess();
        ZB_Reservation.SetEnvironmentClasses(this.DBAdmin, this.Conf, this.Log);
        try {
            ZB_Reservation.CheckReservation_ZB_Stato(ZB.IDBlackBox);
        } catch (SQLException ex) {
            Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (TK.IDToken < 0L) {
            System.out.println(FullDate + "TokenHWID_N Token non trovato");
        } else if (Type == 1) {
            TKAs = new TokenAuths(this.DB);
            TKAs.ReadAuth(ZB.IDBlackBox, TK.IDToken, TK.TokenHWID_N, false, TK);
        } else if (Type == 2) {
            TKAs = new TokenAuths(this.DB);
            TK.IDToken = -1L;
            TK.TokenHWID_N = -1L;
            TKAs.ReadAuth(ZB.IDBlackBox, -1L, -1L, false, TK);
        } else if (Type == 3) {
            TKAs = new TokenAuths(this.DB);
            TK.IDToken = -1L;
            TK.TokenHWID_N = -1L;
            TKAs.ReadAuth(ZB.IDBlackBox, -1L, -1L, true, TK);
        } else if (Type == 4) {
            TKAs = new TokenAuths(this.DB);
            TKAs.ReadAuth(ZB.IDBlackBox, TK.IDToken, TK.TokenHWID_N, true, TK);
        }
        return TKAs;
    }

    byte[] Send_GPRS_COMMAND_ENABLECODEREPLY(zbox ZB, TokenAuths TKAs) {
        int c = GPRS_COMMAND_ENABLECODEREPLY;
        int EnableReplyCount = Math.min(TKAs.Auth.size(), 77);
        boolean DataAvailable = TKAs.Auth.size() > 77;
        byte[] buff = new byte[13 * TKAs.Auth.size() + 2];
        buff[0] = (byte) ZB.PackNRef;
        if (DataAvailable) {
            buff[1] = (byte) (EnableReplyCount | 0x80);
        } else {
            buff[1] = (byte) (EnableReplyCount & 0x7F);
        }

        for (int i = 0; i < EnableReplyCount; i++) {
            byte[] b = TKAs.Auth.get(i).GetBytes();
            System.arraycopy(b, 0, buff, 2 + i * 13, 13);
        }

//        byte Count=DBAdmin.IsCommandWaiting(ZB.IDBlackBox);
//        Log.WriteLog(3,"ID= "+SessionID+" Invio COMMAND_Ok a ZB :"+ new String(Hex.encode(ZB.SerialN))+" NCommand="+Count+" IDBlackBox="+ZB.IDBlackBox);
//        if (Count>0) {
//            buff[1]=Count;
//        }

        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_ENABLECODEREPLY, buff, (byte) 0x01);

        ZB.TKAs_Acknoledge = TKAs;

        return ret;
    }

    byte[] Send_GPRS_COMMAND_ENABLELONGCODEREPLY(zbox ZB, TokenAuths TKAs) {
        int c = GPRS_COMMAND_ENABLECODEREPLY;
        int EnableReplyCount = Math.min(TKAs.Auth.size(), 59);
        boolean DataAvailable = TKAs.Auth.size() > 59;
        byte[] buff = new byte[17 * TKAs.Auth.size() + 2];
        buff[0] = ((byte) ZB.PackNRef);
        if (DataAvailable) {
            buff[1] = ((byte) (EnableReplyCount | 0x80));
        } else {
            buff[1] = ((byte) (EnableReplyCount & 0x7F));
        }
        for (int i = 0; i < EnableReplyCount; i++) {
            byte[] b = TKAs.Auth.get(i).GetBytesFormatMifare();
            System.arraycopy(b, 0, buff, 2 + i * 17, 17);
            System.out.println("Auth " + Utils.toHexString(b));
        }

        byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_ENABLELONGCODEREPLY, buff, (byte) 1);

        ZB.TKAs_Acknoledge = TKAs;

        return ret;
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

        Log.WriteLog(3, "ReadFileUploadStart ID= " + SessionID + " ChunkTot:" + ChunkTot + " filename=" + filename);
        // query insert su zbfileupload
        return DBAdmin.AddStartToZBFileUpload(ZB, FileType, ChunkTot, 1000,  filename, CRC);
    }
    
    
    

    long ReadFileUploadStart_ABOX(zbox ZB, GPRS_packet Pk) {
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

        Log.WriteLog(3, "ReadFileUploadStart_ABOX ID= " + SessionID + " ChunkTot:" + ChunkTot + " filename=" + filename);
        System.out.println("ReadFileUploadStart_ABOX ID= " + SessionID + " ChunkTot:" + ChunkTot + " filename=" + filename);
        // query insert su zbfileupload
        return DBAdmin.AddStartToZBFileUpload(ZB, FileType, ChunkTot, 528, filename, CRC);
    }

    FileTrasfer ReadFileChunkReq(zbox ZB, GPRS_packet Pk) {
        FileTrasfer FT = new FileTrasfer();

        FT.ChunkCurr = (Utils.uBToI(Pk.Payload[4]) << 8) + Utils.uBToI(Pk.Payload[5]);
        FT.IDFile = (Utils.uBToL(Pk.Payload[1]) << 16) + (Utils.uBToL(Pk.Payload[2]) << 8) + Utils.uBToL(Pk.Payload[3]);
        FT.ChunkSize=1000;
        Log.WriteLog(3, "ID= " + SessionID + " Chunk corrente:" + FT.ChunkCurr + " IdFile=" + FT.IDFile);
        return FT;
    }

    FileTrasfer ReadFileChunkReq_ABOX(zbox ZB, GPRS_packet Pk) {
        FileTrasfer FT = new FileTrasfer();

        FT.ChunkCurr = (Utils.uBToI(Pk.Payload[4]) << 8) + Utils.uBToI(Pk.Payload[5]);
        FT.IDFile = (Utils.uBToL(Pk.Payload[1]) << 16) + (Utils.uBToL(Pk.Payload[2]) << 8) + Utils.uBToL(Pk.Payload[3]);
        FT.ChunkSize=528;
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

    FileTrasfer ReadFileChunkData_ABOX(zbox ZB, GPRS_packet Pk) {
        byte[] chunk = new byte[528];
        int i;
        int ByteCount = (Utils.uBToI(Pk.Payload[1]) << 8) + Utils.uBToI(Pk.Payload[2]);
        int ChunkNum = (Utils.uBToI(Pk.Payload[3]) << 8) + Utils.uBToI(Pk.Payload[4]);
        long IdFile = (Utils.uBToL(Pk.Payload[5]) << 16) + (Utils.uBToL(Pk.Payload[6]) << 8) + Utils.uBToL(Pk.Payload[7]);
        Log.WriteLog(3, "ReadFileChunkData_ABOX ID= " + SessionID + " ByteCount:" + ByteCount + " ChunkNum=" + ChunkNum + " IdFile=" + IdFile);
        //System.out.println("ReadFileChunkData_ABOX ID= " + SessionID + " ByteCount:" + ByteCount + " ChunkNum=" + ChunkNum + " IdFile=" + IdFile);
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

    /*
     * definisce comportamenti speciali da associare alle box in caso di ricevzione eventi particolari
     */
    
    boolean ManageSpecialEvents(Connection conn, zbox ZB,ZBRecord ZBRec) throws SQLException{
        if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordE){
            if (ZB.AutoAccFileDownload>0){          // se per la box è previsto il download automatico dei file accelerometrici
                for (int i=0;i<ZBRec.RecE.NumEventi;i++){
                    if (ZBRec.RecE.E[i].TypeEv==2){     //evento grave
                        int NumFile = (Utils.uBToI(ZBRec.RecE.E[i].Extra[1]) << 8) + Utils.uBToI(ZBRec.RecE.E[i].Extra[0]);
                        String command="GPR-Acc:"+NumFile;
                        System.out.println("ManageSpecialEvents IDBlackbox="+ZB.IDBlackBox+ " command - "+command);
                        java.sql.Timestamp Timeout= new java.sql.Timestamp(System.currentTimeMillis()+24*60*60*1000); // 24 ore di timeout
                        DBAdmin.InsertCommand(conn, ZB, command, null, Timeout);
                    }
                }
            }
        }
        return true;
    }
    
    //revisione codice
    private long ReadFileUploadStart(zbox ZB, GPRS_packet Pk, Connection conn) {
        
        try{
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

            Log.WriteLog(3, "ReadFileUploadStart ID= " + SessionID + " ChunkTot:" + ChunkTot + " filename=" + filename);
            // query insert su zbfileupload
            return DBAdmin.AddStartToZBFileUpload(ZB, FileType, ChunkTot, 1000,  filename, CRC, conn);
       
        }catch (Exception e2){
           Log.WriteLog(1, "ReadFileUploadStart Exception: "+e2.toString());
           Log.WriteLog(1, "ReadFileUploadStart:  :"+Utils.getStackTrace(e2));
           return -1;
        } 
    }
    
    //revisione codice
    private long ReadFileUploadStart_ABOX(zbox ZB, GPRS_packet Pk, Connection conn) {
        try{
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

            Log.WriteLog(3, "ReadFileUploadStart_ABOX ID= " + SessionID + " ChunkTot:" + ChunkTot + " filename=" + filename);
            //System.out.println("ReadFileUploadStart_ABOX ID= " + SessionID + " ChunkTot:" + ChunkTot + " filename=" + filename);
            // query insert su zbfileupload
            return DBAdmin.AddStartToZBFileUpload(ZB, FileType, ChunkTot, 528, filename, CRC, conn);
        }catch (Exception e2){
           Log.WriteLog(1, "ReadFileUploadStart_ABOX Exception: "+e2.toString());
           Log.WriteLog(1, "ReadFileUploadStart_ABOX:  :"+Utils.getStackTrace(e2));
           return -1;
        } 
        
    }
    
    
    
    //revisione codice
    private FileTrasfer ReadFileChunkData(zbox ZB, GPRS_packet Pk, Connection conn) {
        
        try{
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

            // query update su zbfileupload
            FileTrasfer FT = DBAdmin.UpdateChunkDataToZBFileUpload(ZB, ByteCount, ChunkNum, IdFile, chunk, conn);
            if (FT!=null){
                FT.ChunkCurr = ChunkNum;
                return FT;
            }else{
                return null;
            }
        }catch (Exception e2){
           Log.WriteLog(1, "ReadFileChunkData Exception: "+e2.toString());
           Log.WriteLog(1, "ReadFileChunkData:  :"+Utils.getStackTrace(e2));
           return null;
        } 
    }
    
    //revisione codice
    private FileTrasfer ReadFileChunkData_ABOX(zbox ZB, GPRS_packet Pk, Connection conn) {
        try{
            byte[] chunk = new byte[528];
            int i;
            int ByteCount = (Utils.uBToI(Pk.Payload[1]) << 8) + Utils.uBToI(Pk.Payload[2]);
            int ChunkNum = (Utils.uBToI(Pk.Payload[3]) << 8) + Utils.uBToI(Pk.Payload[4]);
            long IdFile = (Utils.uBToL(Pk.Payload[5]) << 16) + (Utils.uBToL(Pk.Payload[6]) << 8) + Utils.uBToL(Pk.Payload[7]);
            Log.WriteLog(3, "ReadFileChunkData_ABOX ID= " + SessionID + " ByteCount:" + ByteCount + " ChunkNum=" + ChunkNum + " IdFile=" + IdFile);
            //System.out.println("ReadFileChunkData_ABOX ID= " + SessionID + " ByteCount:" + ByteCount + " ChunkNum=" + ChunkNum + " IdFile=" + IdFile);
            // for (i=1;i<8;i++)
            //    Log.WriteLog(3,"Pk.Payload:["+i+"]"+Pk.Payload[i]);
            for (i = 0; i < ByteCount; i++) {
                chunk[i] = Pk.Payload[8 + i];
            }
    //        System.arraycopy(i, i, i, i, i);
            // query update su zbfileupload
            FileTrasfer FT = DBAdmin.UpdateChunkDataToZBFileUpload(ZB, ByteCount, ChunkNum, IdFile, chunk, conn);
            if (FT!=null){
                FT.ChunkCurr = ChunkNum;
                return FT;
            }else{
                return null;
            }
        }catch (Exception e2){
           Log.WriteLog(1, "ReadFileChunkData_ABOX Exception: "+e2.toString());
           Log.WriteLog(1, "ReadFileChunkData_ABOX:  :"+Utils.getStackTrace(e2));
           return null;
        } 
        
 
    }
    
    //revisione codice
    private byte[] Send_FileUploadEnd(zbox ZB, long IdFile, int ChunkTot, byte ServerNum, Connection conn) {
        byte[] buff = new byte[16];
        int ErrorCode = 50;

        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) ((IdFile >> 16) & 0xFF);
        buff[2] = (byte) ((IdFile >> 8) & 0xFF);
        buff[3] = (byte) (IdFile & 0xFF);
        buff[4] = (byte) ((ChunkTot >> 8) & 0xFF);
        buff[5] = (byte) (ChunkTot & 0xFF);
        if (DBAdmin.VerifyHashUploadFile(ZB, IdFile, conn) == true) {
            DBAdmin.UpdateStatoIntoZBFileUpload(ZB, IdFile, conn);
            ErrorCode = 0;
        }
        buff[6] = (byte) ErrorCode;
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND a ZB :" + new String(Hex.encode(ZB.SerialN))+ " ErrorCode "+ErrorCode);
        //System.out.println( "ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND a ZB :" + new String(Hex.encode(ZB.SerialN))+ " ErrorCode "+ErrorCode);
        

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_FILEUPLOADEND, buff, (byte) 0x01);

        return ret;
    }
    
    private byte[] Send_FileUploadEnd_ABOX(zbox ZB, long IdFile, int ChunkTot, byte ServerNum, Connection conn) {
        byte[] buff = new byte[16];
        int ErrorCode = 50;

        buff[0] = (byte) ZB.PackNRef;
        buff[1] = (byte) ((IdFile >> 16) & 0xFF);
        buff[2] = (byte) ((IdFile >> 8) & 0xFF);
        buff[3] = (byte) (IdFile & 0xFF);
        buff[4] = (byte) ((ChunkTot >> 8) & 0xFF);
        buff[5] = (byte) (ChunkTot & 0xFF);
//        if (DBAdmin.VerifyHashUploadFile(ZB, IdFile) == true) {
            DBAdmin.UpdateStatoIntoZBFileUpload(ZB, IdFile, conn);
            ErrorCode = 0;
//        }
        buff[6] = (byte) ErrorCode;
        Log.WriteLog(3, "ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND_ABOX a ZB :" + new String(Hex.encode(ZB.SerialN)));
        //System.out.println("ID= " + SessionID + " Invio COMMAND_FILEUPLOADEND_ABOX a ZB :" + new String(Hex.encode(ZB.SerialN)));

        byte[] ret = SendGPRSData(ZB, ServerNum, GPRS_COMMAND_FILEUPLOADEND_ABOX, buff, (byte) 0x01);

        return ret;
    }
    
    //revisione codice
    private byte[] Send_FileDownloadInfo(zbox ZB, Connection conn) throws Exception {
        byte[] buff = new byte[32];
//        byte [] filename=new byte[24];
        int i;

        DownloadInfo DwnlInfo;
        DwnlInfo = DBAdmin.ReadDownloadInfo(ZB, conn);
        if (DwnlInfo == null) {
            throw new Exception("File non trovato");
        }
        try{
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
        }catch (Exception e2){
           Log.WriteLog(1, "Send_FileDownloadInfo Exception: "+e2.toString());
           Log.WriteLog(1, "Send_FileDownloadInfo:  :"+Utils.getStackTrace(e2));
           throw e2;
        } 
    }
     
    private byte[] Send_FileDownloadInfo_ABOX(zbox ZB, Connection conn) throws Exception {
        byte[] buff = new byte[32];
//        byte [] filename=new byte[24];
        int i;

        DownloadInfo DwnlInfo;
        DwnlInfo = DBAdmin.ReadDownloadInfo(ZB, conn);
        if (DwnlInfo == null) {
            throw new Exception("File non trovato");
        }
        try{
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

            byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_FILEDOWNLOADINFO_ABOX, buff, (byte) 0x01);
            return ret;
        }catch (Exception e2){
           Log.WriteLog(1, "Send_FileDownloadInfo_ABOX Exception: "+e2.toString());
           Log.WriteLog(1, "Send_FileDownloadInfo_ABOX:  :"+Utils.getStackTrace(e2));
           throw e2;
        } 
    }
    
    //revisione codice
    private byte[] Send_FileChunkData_ABOX(zbox ZB, FileTrasfer FT, Connection conn) {
        
        try{
        
            byte[] buff = new byte[FT.ChunkSize+8];
            int i;

            DownloadChunkData DwnlChkData;
            DwnlChkData = DBAdmin.ReadDownloadChunkData(ZB, FT, conn);
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
            byte[] buff1 = new byte[DwnlChkData.ByteCount+8];
            System.arraycopy(buff, 0, buff1, 0, DwnlChkData.ByteCount+8);
            byte[] ret = SendGPRSData(ZB, (byte) 0, GPRS_COMMAND_FILECHUNKDATA_ABOX, buff1, (byte) 0x01);
            return ret;
        }catch (Exception e2){
           Log.WriteLog(1, "Send_FileChunkData_ABOX Exception: "+e2.toString());
           Log.WriteLog(1, "Send_FileChunkData_ABOX:  :"+Utils.getStackTrace(e2));
           return null;
        }
    }
    //revisione codice
    private byte[] Send_FileChunkData(zbox ZB, FileTrasfer FT, Connection conn) {
        
        try{
            byte[] buff = new byte[1008];
            int i;

            DownloadChunkData DwnlChkData;
            DwnlChkData = DBAdmin.ReadDownloadChunkData(ZB, FT, conn);
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
         }catch (Exception e2){
           Log.WriteLog(1, "Send_FileChunkData Exception: "+e2.toString());
           Log.WriteLog(1, "Send_FileChunkData:  :"+Utils.getStackTrace(e2));
           return null;
         }
    }
}
