/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.Token;
import com.viacom.zbox.Utils;
import com.viacom.zbox.ZBRecord;
import com.viacom.zbox.ZBRecord.RecordTypes;
import com.viacom.zbox.execeptions.ExceptionCRCError;
import com.viacom.zbox.execeptions.ExceptionInvalidRecordLenght;
import com.viacom.zbox.zbox;
import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bouncycastle.util.encoders.Hex;
import snaq.db.ConnectionPool;

/**
 *
 * @author Luca
 */
public class ZB_Record_Parser extends Thread {

    boolean Exit = false;
//    boolean Exit=true;
    public boolean Running = false;
    public int Errors = 0;
    Connection connBB = null;
    ArrayList<zbox> ZB_List;
    ConfClass Conf;
    LogsClass Log;
    DBConnector DBConn;
    DBAdminClass DBAdmin1;

    public ZB_Record_Parser(ArrayList<zbox> ZB_loc) {
        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
        DBConn = DBConnector.getInstance();
        DBAdmin1 = new DBAdminClass();
        DBAdmin1.SetConf(Conf);
        DBAdmin1.SetLog(Log);

        ZB_List = ZB_loc;
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {
        Running = true;
        System.out.println("ZB_Record_Parser: " + Thread.currentThread().getName() + " Start. ");
        try {
            InitDB_BB();
            DBAdmin1.DbAdminConn = connBB;

            for (zbox ZB : ZB_List) {
                ProcessData(ZB);
                connBB.commit();
            }

            connBB.close();
        } catch (SQLException ex) {
            Logger.getLogger(ZB_Record_Parser.class.getName()).log(Level.SEVERE, null, ex);
            try {
                connBB.rollback();
            } catch (SQLException ex1) {
                Logger.getLogger(ZB_Record_Parser.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Errors++;
        } catch (Exception ex) {
            Logger.getLogger(ZB_Record_Parser.class.getName()).log(Level.SEVERE, null, ex);
            try {
                connBB.rollback();
            } catch (SQLException ex1) {
                Logger.getLogger(ZB_Record_Parser.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Errors++;
        }
        System.out.println("ZB_Record_Parser: " + Thread.currentThread().getName() + " END. ");
        Running = false;
    }

    public int ProcessData(zbox ZB) throws SQLException, Exception {
        PreparedStatement statement, statement1;
        ResultSet rs;
        int RecordCount = 0;

        sleep(1500);
        Timestamp FullStartTime1 = new Timestamp(new Date().getTime());

        String QueryString = "SELECT R.IDBlackBox, R.Record, R.Stato, R.IDRec, R.Time,R.IDRec from ZBRecords R "

//        String QueryString = "SELECT R.IDBlackBox, R.Record, R.Stato, R.IDRec, R.Time,B.IDAzienda, B.BBserial,R.IDRec,B.IDSWVersion from ZBRecords R"
//                + " LEFT JOIN BlackBox B ON R.IDBlackBox=B.IDBlackBox"
                + " WHERE Stato=0 and R.Time< DATE_SUB(NOW(), INTERVAL 10 SECOND)"
                + " and R.IDBlackBox=" + ZB.IDBlackBox
                + " ORDER BY R.IDRec LIMIT 50";
        statement = connBB.prepareStatement(QueryString);
        rs = statement.executeQuery();
        Timestamp FullStartTime2 = new Timestamp(new Date().getTime());

        while (rs.next()) {
            RecordCount++;
            Timestamp RecStartTime = new Timestamp(new Date().getTime());


            long IDRec = rs.getLong("IDRec");

            byte Rec[] = rs.getBytes("Record");
            byte Type = (byte) (Rec[0] >> 4);

            if (Rec.length < 48) {
                Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " : dato non corretto");
                UpdateZBRecordStato(connBB, rs.getLong("IDRec"), 3);
                continue;
            } else {
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
                if (CRC[0] != Rec[47] && ZB.IDSWVersion > 17) {
                    Log.WriteLog(4, "Errore sulla verifica del CRC IDRec=" + rs.getLong("IDRec"));
                    UpdateZBRecordStato(connBB, rs.getLong("IDRec"), 50);
                } else {

                    ZBRecord ZBRec = new ZBRecord();
                    try {
                        ZBRec.ParseRecord(rs.getLong("IDRec"), ZB.IDBlackBox, Rec, rs.getTimestamp("Time"), ZB.IDSWVersion > 17);
                    } catch (ExceptionInvalidRecordLenght ex) {
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        UpdateZBRecordStato(connBB, rs.getLong("IDRec"), 51);
                        continue;
                    } catch (ExceptionCRCError ex) {
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        UpdateZBRecordStato(connBB, rs.getLong("IDRec"), 50);
                        continue;
                    } catch (Exception ex) {
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        continue;
                    }

                    if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordRT) { //record RealTime
                        Log.WriteLog(3, "ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong("IDRec") + " Rec RT");
                        try {
                            DBAdmin1.InsertZBRecord(connBB, ZBRec);
                        } catch (SQLException ex) {
                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            connBB.rollback();
                            return 0;
                        }
                    } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordE) {       //  Record Evento
                        Log.WriteLog(3, "ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong("IDRec") + " Rec Evento");
                        try {
                            DBAdmin1.InsertZBRecord(connBB, ZBRec);
                        } catch (SQLException ex) {
                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            connBB.rollback();
                            return 0;
                        }
                    } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordY) {       //  Record Y
                        String LogOut = "ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong(4) + " Rec Y ";
                        Timestamp StartTime = new Timestamp((new java.util.Date()).getTime());
                        int ret = ReadRecY(Rec, rs.getLong(4), (int) ZB.IDBlackBox, rs.getTimestamp(5));
                        Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
                        long TimeDiff = now.getTime() - StartTime.getTime();
                        LogOut += " TimeDiff=" + TimeDiff;
                        Log.WriteLog(3, LogOut);
                        if (ret == 0) {
                            connBB.rollback();
                            return 0;
                        }
                    } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordX) {       //  Record X
                        try {
                            int TypeEv = DBAdminClass.uBToI(Rec[1]);
                            Log.WriteLog(3, "ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong(4) + " Rec X Type " + TypeEv + " ");
                            UpdateZBRecordStato(connBB, IDRec, 33);
                        } catch (SQLException ex) {
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            connBB.rollback();
                            return 0;
                        } catch (Exception ex) {
                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            connBB.rollback();
                            return 0;
                        }

                    } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordZ) {       //  Record Z
                        Timestamp StartTime = new Timestamp((new java.util.Date()).getTime());
                        Log.WriteLog(3, "ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong(4) + " Rec Z Type ");
                        int IDGuida = ((DBAdminClass.uBToI(Rec[0]) & 0xF) << 16) + (DBAdminClass.uBToI(Rec[1]) << 8) + DBAdminClass.uBToI(Rec[2]);
                        byte B[] = new byte[8];
                        System.arraycopy(Rec, 4, B, 0, 4);
                        Timestamp TS = ZBRecord.GetTimeStamp(B);
                        if (TS == null) {
                            TS = rs.getTimestamp(5);
                        }
                        if (TS != null) {
                            System.out.print("Timestamp=" + TS.toString());
                        }
                        System.arraycopy(Rec, 8, B, 0, 4);
                        Double Lat = ZBRecord.GetCoord(B);
                        System.arraycopy(Rec, 12, B, 0, 4);
                        Double Long = ZBRecord.GetCoord(B);
                        int ContaKm = (DBAdminClass.uBToI(Rec[19]) << 24) + (DBAdminClass.uBToI(Rec[18]) << 16) + (DBAdminClass.uBToI(Rec[17]) << 8) + DBAdminClass.uBToI(Rec[16]);
                        //UCode (TODO)
                        byte TokenSN[] = new byte[4];
                        System.arraycopy(Rec, 20, TokenSN, 0, 4);
                        System.out.println(" Codice Conducente:" + (new String(Hex.encode(TokenSN))).toUpperCase());
                        int IDAzienda =  ZB.IDAzienda;
                        Token TK = FindIDToken(TokenSN, IDAzienda);
                        if (TK.IDToken == -1) {
                            TK.IDToken = InsertToken(TK);
                        }
                        // minuti validi nel record
                        int NumMinutes = DBAdminClass.uBToI(Rec[24]) + 1;
                        if (NumMinutes > 4) {
                            NumMinutes = 4;
                        }
                        // durata intervallo
                        int DurataIntervallo = DBAdminClass.uBToI(Rec[25]);
                        // FuelLevel
                        int FuelLevel = DBAdminClass.uBToI(Rec[46]);

                        try {
                            QueryString = "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=? and ReceiveComplete!=1";
                            statement1 = connBB.prepareStatement(QueryString);
                            statement1.setLong(1, ZB.IDBlackBox);  //IDBlackBox
                            statement1.execute();
                            statement1.close();
                            QueryString = "INSERT INTO ZBRecZ (IDBlackBox, IDGuida, ContaKm, FuelLevel, BLat, BLong, BTimeStamp,"
                                    + "IDToken, IDRecord, DurataIntervallo) VALUES ("
                                    + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                            statement1 = connBB.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                            statement1.setLong(1, ZB.IDBlackBox);  //IDBlackBox
                            statement1.setInt(2, IDGuida);
                            statement1.setInt(3, ContaKm);
                            statement1.setInt(4, FuelLevel);
                            statement1.setDouble(5, Lat);
                            statement1.setDouble(6, Long);
                            statement1.setTimestamp(7, TS);
                            statement1.setLong(8, TK.IDToken);
                            statement1.setLong(9, IDRec);    //IDRec
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
                                QueryString = "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=? and ReceiveComplete!=1";
                                statement1 = connBB.prepareStatement(QueryString);
                                statement1.setLong(1, ZB.IDBlackBox);  //IDBlackBox
                                statement1.execute();
                                statement1.close();
                            }
                            UpdateZBRecordStato(connBB, IDRec, 1);
                        } catch (SQLException ex) {
                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            UpdateZBRecordStato(connBB, IDRec, 4);
                            //                        DbAdminConn.rollback();
                            return 0;
                        } catch (Exception ex) {
                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            UpdateZBRecordStato(connBB, IDRec, 4);
                            //                        DbAdminConn.rollback();
                            return 0;
                        }
                        Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
                        long TimeDiff = now.getTime() - StartTime.getTime();
                        //                    System.out.println("TimeDiff="+TimeDiff);
                        System.out.println("ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong(4) + " Rec Z Type TimeDiff=" + TimeDiff);

                    } else if (Type == 5) {       //  Record T
                        Log.WriteLog(3, "ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong(4) + " Rec T ");
                        //                    long IDRec=rs.getLong(4);
                        int ret;
                        ret = ReadRecT(Rec, IDRec, ZB.IDBlackBox, rs.getTimestamp(5));
                        if (ret == 0) {
                            connBB.rollback();
                            return 0;
                        }
                    } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordI) {       //  Record I
                        Log.WriteLog(3, "ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong(4) + " Rec I ");
                        try {
                            DBAdmin1.InsertZBRecord(connBB, ZBRec);
                        } catch (SQLException ex) {
                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            connBB.rollback();
                            return 0;
                        }
                    } else {
                        try {
                            UpdateZBRecordStato(connBB, IDRec, 3);
                        } catch (SQLException ex) {
                            Log.WriteEx(DBAdminClass.class.getName(), ex);
                            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                            connBB.rollback();
                            return 0;
                        }
                    }
                    Timestamp now = new Timestamp(new Date().getTime());
                    long RecTimeDiff = now.getTime() - RecStartTime.getTime();

                    System.out.println("ZB=" + Utils.toHexString(ZB.SerialN) + " IDRecord =" + rs.getLong(4) + " Type=" + Type + " TimeDiff=" + RecTimeDiff);
                }
            }
        }
        Timestamp now = new Timestamp(new Date().getTime());
        long RecTimeDiff1 = now.getTime() - FullStartTime1.getTime();
        long RecTimeDiff2 = now.getTime() - FullStartTime2.getTime();

        System.out.println("ZB=" + Utils.toHexString(ZB.SerialN) + " RecordCount=" + RecordCount + " full TimeDiff1=" + RecTimeDiff1 + " TimeDiff2=" + RecTimeDiff2);
        return 1;
    }

    public static ArrayList<ArrayList<zbox>> GetZBoxListGroup(Connection conn, int NumMax) {
        Statement statement;
        ResultSet rs;

        ArrayList<ArrayList<zbox>> ZBGroup;
        ZBGroup = (ArrayList<ArrayList<zbox>>) new ArrayList<ArrayList<zbox>>();
//        for (int i=0;i<Num;i++) {
//            ZBGroup[i]=new ArrayList<zbox>();
//        }

        try {
            // esegue la query al DB
            statement = conn.createStatement();
//            String QueryString = "Select * from(select IDBlackBox,count(*) C from ZBRecords where Stato=0 "
//                    + "group by IDBlackBox order by C Desc) A where A.C<20 limit 100";
//            String QueryString = "select IDBlackBox,count(*) C from ZBRecords where Stato=0 "
//                    + "group by IDBlackBox order by C Desc LIMIT 200";
            String QueryString = "select R.*,BB.IDAzienda,BB.IDSWVersion,BB.BBSerial from "
                    + "( select IDBlackBox,count(*) C from ZBRecords where Stato=0  group by IDBlackBox order by C Desc LIMIT 200) R "
                    + "left join BlackBox BB on BB.IDBlackBox=R.IDBlackBox";

//            String QueryString = "select IDBlackBox,count(*) C from ZBRecords where Stato=0 and IDBlackBox=2318 group by IDBlackBox order by C Desc LIMIT 200";
//            String QueryString = "select IDBlackBox,count(*) C from ZBRecords where Stato=0 and IDBlackBox in (9226,7054,4483,9196,1174,3329,9139,838,2473,6551,6134) group by IDBlackBox order by C Desc LIMIT 200";
//            String QueryString = "select R.IDBlackBox,count(*) C from ZBRecords R left join BlackBox B on B.IDBlackBox=R.IDBlackBox "
//                    + "where Stato=0 and B.IDAzienda=43 group by IDBlackBox order by C Desc LIMIT 200";
//            String QueryString = "SELECT Distinct A.IDBlackBox FROM (SELECT * FROM blackbox_debug.zbrecords where stato =0 order by IDRec limit 1300) A";

            rs = statement.executeQuery(QueryString);
            int Counter = 0;
            int TotRec = 0;

            while (rs.next()) {
                TotRec += rs.getInt("C");
                Counter++;
            }

            int Num = Math.min(NumMax, Math.min(TotRec / 15, Counter));
            System.out.println("Num=" + Num);
            Counter = 0;
            if (Num == 0) {
                try {
                    sleep(10000L);
                } catch (InterruptedException ex) {
                    Logger.getLogger(ZB_Record_Parser.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                rs.beforeFirst();
                while (rs.next()) {
                    zbox ZB = new zbox();
                    ZB.IDBlackBox = rs.getInt("IDBlackBox");
                    ZB.IDAzienda = rs.getInt("IDAzienda");
                    ZB.IDSWVersion = rs.getInt("IDSWVersion");
                    ZB.SerialN = Hex.decode(rs.getString("BBSerial"));
                    ArrayList<zbox> Inner;

                    if (Counter % Num == Counter) {
                        Inner = new ArrayList<zbox>();
                        Inner.add(ZB);
                        ZBGroup.add(Inner);
                    } else {
                        ZBGroup.get(Counter % Num).add(ZB);
                    }

                    Counter++;
                }
            }
            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(ZB_Record_Parser.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return ZBGroup;
    }

    boolean InitDB_BB() throws Exception {
        try {
            connBB = DBConn.PoolBB.getConnection();

            connBB.setAutoCommit(false);
        } catch (Exception e) {
            Log.WriteLog(0, "ZB_Record_Parser.InitDB_BB():Cannot connect to database server: " + e.getMessage());
            throw e;
        }
        return true;
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

    /**
     * Legge ed archivia i record T nel DB
     *
     * @param Rec: record da archiviare
     * @param IDRecord: identificativo del record nel DB
     * @param IDBlackBox: identificativo della ZB
     * @param TS: Timestamp di ricezione del record
     * @return
     */
    public int ReadRecT(byte Rec[], long IDRecord, long IDBlackBox, Timestamp TS) 
            throws SQLException, Exception 
    {
        PreparedStatement statement;
        try {
            String QueryString;

            // primo blocco di telemetrie
            int Payload1Size = (DBAdminClass.uBToI(Rec[3]) & 0x0F);
            int TipoTelemetria1 = ((DBAdminClass.uBToI(Rec[3]) & 0xF0) >> 4);
            long PID1 = ((DBAdminClass.uBToI(Rec[11]) & 0xFF) << 24)
                    + ((DBAdminClass.uBToI(Rec[10]) & 0xFF) << 16)
                    + ((DBAdminClass.uBToI(Rec[9]) & 0xFF) << 8)
                    + +(DBAdminClass.uBToI(Rec[8]) & 0xFF);
            byte Payload1[] = new byte[12];
            if (Payload1Size > 12
                    || (TipoTelemetria1 != 0 && TipoTelemetria1 != 1 && TipoTelemetria1 != 2 && TipoTelemetria1 != 3
                    && TipoTelemetria1 != 4 && TipoTelemetria1 != 5)) {
                Log.WriteLog(1, "Errore nel record Telemetria IDRec=" + IDRecord);
                UpdateZBRecordStato(connBB, IDRecord, 3);
                return 1;
            }
            System.arraycopy(Rec, 12, Payload1, 0, Payload1Size);
            if (TipoTelemetria1 == 3) {        // sensore termico
                int Temp[] = new int[6];
                int i;
                for (i = 0; i < 6; i++) {
                    if ((DBAdminClass.uBToI(Payload1[2 * i + 1]) & 0x80) == 0x80) {
                        Temp[i] = -(((Utils.uBToI(Payload1[2 * i + 1]) & 0x7F) << 8) + Utils.uBToI(Payload1[2 * i]));
                    } else {
                        Temp[i] = ((Utils.uBToI(Payload1[2 * i + 1]) & 0x7F) << 8) + Utils.uBToI(Payload1[2 * i]);
                    }
                }
                Log.WriteLog(3, "Temp[0]=" + Temp[0] + " Temp[1]=" + Temp[1] + " Temp[2]=" + Temp[2] + " Temp[3]=" + Temp[3] + " Temp[4]=" + Temp[4] + " Temp[5]=" + Temp[5]);
            } else if (TipoTelemetria1 == 4) {        // TPMS
                int Payload2Size = (DBAdminClass.uBToI(Rec[44]) & 0x0F);

                Payload1 = new byte[24];
                System.arraycopy(Rec, 12, Payload1, 0, Payload1Size);
                System.arraycopy(Rec, 32, Payload1, 12, Payload2Size);
            } else if (TipoTelemetria1 == 5) {        // HHO
                int Payload2Size = (DBAdminClass.uBToI(Rec[44]) & 0x0F);

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
                System.out.print("Timestamp=" + BTime.toString());
            }

            QueryString = "INSERT INTO ZBTelemetry (IDBlackBox, Type, PID, Payload, Stato, IDRecord , BTimeStamp) VALUES ("
                    + " ?, ?, ?,?, 0, ?, ?)";

            statement = connBB.prepareStatement(QueryString);
            statement.setLong(1, IDBlackBox);
            statement.setInt(2, TipoTelemetria1);
            statement.setLong(3, PID1);
            statement.setBytes(4, Payload1);
            statement.setLong(5, IDRecord);
            statement.setTimestamp(6, BTime);

            statement.execute();
            statement.close();

            // secondo blocco

            UpdateZBRecordStato(connBB, IDRecord, 1);

        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } catch (Exception ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            UpdateZBRecordStato(connBB, IDRecord, 3);
            throw ex;
        }
        return 1;
    }

    public int ReadRecY(byte Rec[], long IDRecord, int IDBlackBox, Timestamp TS) {
        PreparedStatement statement, statement1;
        try {
            int IDGuida = ((DBAdminClass.uBToI(Rec[0]) & 0xF) << 16) + (DBAdminClass.uBToI(Rec[1]) << 8) + DBAdminClass.uBToI(Rec[2]);

            // minuti validi nel record
            int NumMinutes = DBAdminClass.uBToI(Rec[3]);
            if (NumMinutes > 8) {
                NumMinutes = 8;
            }
            int FuelLevel = DBAdminClass.uBToI(Rec[44]);

            // cerco il record Z di riferimento
            String QueryString = "SELECT RZ.IDZBRecZ, RZ.IDBlackBox, RZ.IDGuida, RZ.IDRecord, RR.Time, "
                    + "RXY.SeqNum, RZ.CertidriveIDGuida from ZBRecz RZ "
                    + "LEFT JOIN ZBRecords RR ON RR.IDRec=RZ.IDRecord "
                    + "LEFT JOIN ZBRecXY RXY ON RXY.IDZBRecZ=RZ.IDZBRecZ "
                    + "WHERE ((RZ.IDGuida=?) and (RR.Time<=?) and (RZ.IDBlackBox=?)) "
                    + "ORDER BY RR.Time desc, RXY.SeqNum desc limit 1";

            statement = connBB.prepareStatement(QueryString);
            statement.setInt(1, IDGuida);
            statement.setTimestamp(2, TS);
            statement.setInt(3, IDBlackBox);
            ResultSet rs1;
            rs1 = statement.executeQuery();
            if (rs1.next()) {   // se è stato trovato il record Z di riferimento
                int IDZBRecZ = rs1.getInt(1);
                int SeqNum = rs1.getInt(6) + 1;
                for (int i = 0; i < NumMinutes; i++) {
                    byte RecTemp[] = new byte[5];
                    System.arraycopy(Rec, 4 + (i * 5), RecTemp, 0, 5);
                    ReadRecordTemp(RecTemp, IDZBRecZ, SeqNum + i, FuelLevel, IDRecord);
                }
                QueryString = "UPDATE ZBRecZ SET Stato=0 WHERE IDZBRecZ=?";
                statement1 = connBB.prepareStatement(QueryString);
                statement1.setLong(1, IDZBRecZ);    //IDRec
                statement1.execute();
                statement1.close();
                if (NumMinutes < 8) {
                    QueryString = "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=? and ReceiveComplete!=1";
                    statement1 = connBB.prepareStatement(QueryString);
                    statement1.setInt(1, IDBlackBox);  //IDBlackBox
                    statement1.execute();
                    statement1.close();
                }
            } else {
                Log.WriteLog(2, "IDRec=" + IDRecord + " Cannot connect record Y to relative Z");
            }
            rs1.close();
            statement.close();

            UpdateZBRecordStato(connBB, IDRecord, 1);
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

    public int ReadRecX(byte Rec[], long IDRecord, int IDBlackBox, Timestamp TS) {

        return 1;
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
            int DistanzaPercorsa = ((DBAdminClass.uBToI(RecTemp[0])) << 4) + ((DBAdminClass.uBToI(RecTemp[1]) >> 4) & 0xF);
            int DeltaLat = ((DBAdminClass.uBToI(RecTemp[1]) & 0x7) << 8) + DBAdminClass.uBToI(RecTemp[2]);
            if (((DBAdminClass.uBToI(RecTemp[1]) & 0x8) >> 3) > 0) {
                DeltaLat = -(DeltaLat);
            }

            int DeltaLong = (((DBAdminClass.uBToI(RecTemp[3])) & 0x7F) << 4) + ((DBAdminClass.uBToI(RecTemp[4]) >> 4) & 0xF);
            if (((DBAdminClass.uBToI(RecTemp[3]) & 0x80) >> 7) > 0) {
                DeltaLong = -(DeltaLong);
            }

            int Eventi = (DBAdminClass.uBToI(RecTemp[4]) & 0xF);
            try {
                String QueryString = "INSERT INTO ZBRecXY (IDZBRecZ, SeqNum, Metri, DeltaLat, DeltaLong, Eventi, FuelLevel,IDRecord) VALUES ("
                        + " ?, ?, ?, ?, ?, ?, ?,?)";
                statement = connBB.prepareStatement(QueryString);
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

    public Token FindIDToken(byte[] SNToken, int IDAzienda) {
        PreparedStatement statement;
        ResultSet rs;
        Token TK = new Token();
        TK.IDAzienda = IDAzienda;
        TK.SetTokenHexString((new String(Hex.encode(SNToken))).toUpperCase());

        try {
            // esegue la query al DB
            String QueryString = "select IDToken, IDAzienda, Cognome, Nome, TokenHWID, TokenHWID_N, IDTokenType, CertidriveIDConducente"
                    + " from Token WHERE TokenHWID='" + (new String(Hex.encode(SNToken))).toUpperCase() + "'"
                    + "     AND IDAzienda=? AND IDTokenType=2;";

            statement = connBB.prepareStatement(QueryString);
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

    public long InsertToken(Token TK) {
        PreparedStatement statement;

        try {
            String QueryString = "INSERT INTO Token (IDAzienda, TokenHWID, IDTokenType, CertSerialNum, NumProgress,TokenHWID_N)"
                    + " VALUES (?,?,2,0,0,? )";

            statement = connBB.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
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
}
