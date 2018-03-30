/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import ZB_DataAnalisysPack.ZB_DataAnalisys;
import ZB_DataArchivingPack.ZB_DataArchiving;
import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.Token;
import com.viacom.zbox.Utils;
import com.viacom.zbox.ZBRecord;
import com.viacom.zbox.execeptions.ExceptionCRCError;
import com.viacom.zbox.execeptions.ExceptionInvalidRecordLenght;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bouncycastle.util.encoders.Hex;

/**
 *
 * @author Luca
 */
public class ZB_ZBRecordParser {

    DBAdminClass DBAdmin;
    ConfClass Conf;
    LogsClass Log;
    ZB_SWUpdateProcess ZB_SWUpdate;
    ZB_DataAnalisys ZB_DataAnalizer;
    ZB_DataArchiving ZB_DataArchiver;

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin) {
        DBAdmin = LDBAdmin;
        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
        return true;
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
            // esegue la query al DB
            String QueryString = "SELECT R.IDBlackBox, R.Record, R.Stato, R.IDRec, R.Time, B.IDAzienda, B.BBserial,R.IDRec,B.IDSWVersion from ZBRecords R"
                    + " LEFT JOIN BlackBox B ON R.IDBlackBox=B.IDBlackBox"
                    + " WHERE Stato=0 and R.Time< DATE_SUB(NOW(), INTERVAL 1 MINUTE)"
                    //                    + " and R.IDBlackBox=68"
                    //                    + " and R.IDRec=110085088"
                    //                    + "  AND B.IDAzienda<>30"
                    + " ORDER BY R.IDBlackBox,R.IDRec"
                    + " LIMIT 100";

            statement = DBAdmin.DbAdminConn.prepareStatement(QueryString);
            rs = statement.executeQuery();
            while (rs.next()) {
                byte Rec[] = rs.getBytes("Record");
                int IDBlackBox = rs.getInt("IDBlackBox");
                int IDSWVersion = rs.getInt("IDSWVersion");
                // TODO Verificare il Checksum del record
                byte Type = (byte) (Rec[0] >> 4);

                ZBRecord ZBRec = new ZBRecord();
                try {
                    ZBRec.ParseRecord(rs.getLong("IDRec"), IDBlackBox, Rec, rs.getTimestamp(5), IDSWVersion > 17);
                } catch (ExceptionInvalidRecordLenght ex) {
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    DBAdmin.UpdateZBRecordStato(DBAdmin.DbAdminConn, rs.getLong("IDRec"), 51);
                    continue;
                } catch (ExceptionCRCError ex) {
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    Log.WriteLog(4, "Errore sulla verifica del CRC IDRec=" + rs.getLong("IDRec"));
                    DBAdmin.UpdateZBRecordStato(DBAdmin.DbAdminConn, rs.getLong("IDRec"), 50);
                    continue;
                } catch (Exception ex) {
                    Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                    continue;
                }

//                if (Type==4) { //record RealTime
                if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordRT) { //record RealTime
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong("IDRec") + " Rec RT");
                    try {

                        DBAdmin.InsertZBRecord(DBAdmin.DbAdminConn, ZBRec);
                    } catch (SQLException ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DBAdmin.DbAdminConn.rollback();
                        return 0;
                    }
                } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordE) {       //  Record Evento
/*                    int TypeEv=Utils.uBToI(Rec[1]);
                     Log.WriteLog(3,"ZB="+rs.getString("BBserial") +" IDRecord ="+rs.getLong(4)+" Rec Ev Type "+TypeEv+" ");
                     byte B[]=new byte[8];
                     System.arraycopy(Rec, 2, B, 0, 4);
                     Timestamp TS=GetTimeStamp(B);
                     if (TS==null) TS=rs.getTimestamp(5);
                     if (TS!=null) System.out.print("Timestamp="+TS.toString());
                     System.arraycopy(Rec, 6, B, 0, 4);
                     Double Lat= GetCoord(B);
                     System.arraycopy(Rec, 10, B, 0, 4);
                     Double Long= GetCoord(B);
                     byte Extra[]= new byte[10];
                     System.arraycopy(Rec, 14, Extra, 0, 10);
                     if (TypeEv==0) {
                     Log.WriteLog(3," Distress");
                     } else if(TypeEv==1) {
                     Log.WriteLog(3," Accelerometrico Lieve");
                     }else if(TypeEv==2) {
                     Log.WriteLog(3," Accelerometrico Grave");
                     }else if(TypeEv==3) {
                     Log.WriteLog(3," Tamper");
                     }else if(TypeEv==4) {
                     Log.WriteLog(3," Sicurezza");
                     }else if(TypeEv==5) {
                     Log.WriteLog(3," Aux");
                     }else if(TypeEv==6) {
                     Log.WriteLog(3," Assenza Alimentazione Primaria");
                     }else if(TypeEv==7) {
                     Log.WriteLog(3," Ripristino Alimentazione Primaria");
                     }else if(TypeEv==8) {
                     Log.WriteLog(3," Warning Superamento Velocità");
                     }else if(TypeEv==9) {
                     Log.WriteLog(3," Warning Superamento Distanza");
                     }else if(TypeEv==10) {
                     Log.WriteLog(3," na");
                     }else if(TypeEv==11) {
                     System.out.print(" na");
                     }else if(TypeEv==12) {
                     Log.WriteLog(3," Autenticazione Conducente");
                     }else if(TypeEv==13) {
                     Log.WriteLog(3," Spostamento a motore spento");
                     }
                     //System.out.println("");
                     Log.WriteLog(3," Lat="+Lat.toString()+" Long="+Long.toString());
                     try {
                     QueryString = "INSERT INTO ZBEvents (IDBlackBox,IDType, BLat, BLong, BTimeStamp,Extra,IDRecord) VALUES ("
                     + " ?, ?, ?, ?,?,?,?)";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setInt(1, IDBlackBox);
                     statement1.setInt(2, TypeEv);
                     statement1.setDouble(3, Lat);
                     statement1.setDouble(4, Long);
                     statement1.setTimestamp(5, TS);
                     statement1.setBytes(6, Extra);
                     statement1.setLong(7, rs.getLong(4));
                     statement1.execute();
                     QueryString =  "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setLong(1,rs.getLong(4));    //IDRec
                     statement1.execute();
                     statement1.close();
                     } catch (SQLException ex) {
                     Log.WriteEx(DBAdminClass.class.getName(), ex);
                     Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                     DbAdminConn.rollback();
                     return 0;
                     }  
                     */
                } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordY) {       //  Record Y
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec Y ");
                    int ret = DBAdmin.ReadRecY(Rec, rs.getLong(4), IDBlackBox, rs.getTimestamp(5));
                    if (ret == 0) {
                        DBAdmin.DbAdminConn.rollback();
                        return 0;
                    }



                } else if (Type == 1) {       //  Record X
 /*                   try {
                     int TypeEv=uBToI(Rec[1]);
                     Log.WriteLog(3,"ZB="+rs.getString("BBserial") +" IDRecord ="+rs.getLong(4)+" Rec X Type "+TypeEv+" ");
                     QueryString =  "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setLong(1,rs.getLong(4));    //IDRec
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
                     */
                } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordZ) {       //  Record Z
/*                    Log.WriteLog(3,"ZB="+rs.getString("BBserial") +" IDRecord ="+rs.getLong(4)+" Rec Z Type ");
                     int IDGuida= ((uBToI(Rec[0])&0xF)<<16)+(uBToI(Rec[1])<<8)+uBToI(Rec[2]);
                     byte B[]=new byte[8];
                     System.arraycopy(Rec, 4, B, 0, 4);
                     Timestamp TS=GetTimeStamp(B);
                     if (TS==null) TS=rs.getTimestamp(5);
                     if (TS!=null) System.out.print("Timestamp="+TS.toString());
                     System.arraycopy(Rec, 8, B, 0, 4);
                     Double Lat= GetCoord(B);
                     System.arraycopy(Rec, 12, B, 0, 4);
                     Double Long= GetCoord(B);
                     int ContaKm= (uBToI(Rec[19])<<24)+(uBToI(Rec[18])<<16)+(uBToI(Rec[17])<<8)+uBToI(Rec[16]);
                     //UCode (TODO)
                     byte TokenSN[]=new byte[4];
                     System.arraycopy(Rec, 20, TokenSN, 0, 4);
                     System.out.println(" Codice Conducente:"+(new String (Hex.encode(TokenSN))).toUpperCase());
                     int IDAzienda= rs.getInt("IDAzienda");
                     Token TK=FindIDToken(TokenSN,IDAzienda);
                     // minuti validi nel record
                     int NumMinutes= uBToI(Rec[24])+1;
                     if (NumMinutes>4) NumMinutes=4;
                     // durata intervallo
                     int DurataIntervallo= uBToI(Rec[25]);
                     // FuelLevel
                     int FuelLevel=uBToI(Rec[46]);
                    
                     try {
                     QueryString =  "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=?";
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
                     statement1.setInt(8, TK.IDToken);
                     statement1.setLong(9, rs.getLong(4));    //IDRec
                     statement1.setInt(10, DurataIntervallo);
                     statement1.execute();
                     ResultSet generatedKeys;
                     generatedKeys = statement1.getGeneratedKeys();
                     long IDRecZ;
                     if (generatedKeys.next()) {
                     IDRecZ=generatedKeys.getLong(1);
                     for (int i=0;i<NumMinutes;i++) {
                     byte RecTemp []= new byte[5];
                     System.arraycopy(Rec, 26+(i*5), RecTemp, 0, 5);
                     ReadRecordTemp(RecTemp, IDRecZ, i, FuelLevel, rs.getLong(4));
                     }
                     }
                     if (NumMinutes<4) {
                     QueryString =  "UPDATE ZBRecZ SET ReceiveComplete=1 WHERE IDBlackBox=?";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setInt(1, IDBlackBox);  //IDBlackBox
                     statement1.execute();
                     statement1.close();
                     }
                     */                        // inserisco i dati della guida nel DB di Certidrive
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
                    /*                       QueryString =  "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setLong(1,rs.getLong(4));    //IDRec
                     statement1.execute();
                     statement1.close();
                     } catch (SQLException ex) {
                     Log.WriteEx(DBAdminClass.class.getName(), ex);
                     Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                     QueryString =  "UPDATE ZBRecords SET Stato=4 WHERE IDRec=?";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setLong(1,rs.getLong(4));    //IDRec
                     statement1.execute();
                     statement1.close();
                     //                        DbAdminConn.rollback();
                     return 0;
                     } catch (Exception ex) {
                     Log.WriteEx(DBAdminClass.class.getName(), ex);
                     Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                     QueryString =  "UPDATE ZBRecords SET Stato=4 WHERE IDRec=?";
                     statement1 = DbAdminConn.prepareStatement(QueryString);
                     statement1.setLong(1,rs.getLong(4));    //IDRec
                     statement1.execute();
                     statement1.close();
                     //                        DbAdminConn.rollback();
                     return 0;
                     }
                     */
                } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordT) {       //  Record T
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec T ");
                    long IDRec = rs.getLong(4);
                    int ret;
                    ret = DBAdmin.ReadRecT(Rec, IDRec, IDBlackBox, rs.getTimestamp(5));
                    if (ret == 0) {
                        DBAdmin.DbAdminConn.rollback();
                        return 0;
                    }
                } else if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordI) {       //  Record I
                    Log.WriteLog(3, "ZB=" + rs.getString("BBserial") + " IDRecord =" + rs.getLong(4) + " Rec I ");
                    long IDRec = rs.getLong(4);
                    int ret;
                    ret = DBAdmin.ReadRecI(Rec, IDRec, IDBlackBox, rs.getTimestamp(5));
                    if (ret == 0) {
                        DBAdmin.DbAdminConn.rollback();
                        return 0;
                    }
                } else {
                    try {
                        QueryString = "UPDATE ZBRecords SET Stato=3 WHERE IDRec=?";
                        statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                        statement1.setLong(1, rs.getLong(4));    //IDRec
                        statement1.execute();
                        statement1.close();
                    } catch (SQLException ex) {
                        Log.WriteEx(DBAdminClass.class.getName(), ex);
                        Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
                        DBAdmin.DbAdminConn.rollback();
                        return 0;
                    }
                }
            }
            rs.close();
            statement.close();
            /*            if (CDDB!=null) {
             if (!CDDB.isClosed()) {
             CDDB.commit();
             CDDB.close();
             }
             }*/
            DBAdmin.DbAdminConn.commit();
            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }
}
