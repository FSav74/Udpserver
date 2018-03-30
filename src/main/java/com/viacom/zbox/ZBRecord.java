/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

//import com.sun.org.apache.xml.internal.security.utils.Base64;
import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.execeptions.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;

import org.bouncycastle.util.encoders.Hex;
import zb_udp_server.LogsClass;



/**
 *
 * @author Luca
 */
public class ZBRecord {

    public long IDBlackBox = -1;
    public long IDRec = -1;
    public Timestamp RecordReceivingTime;
    
    //mi salvo i zbrecord in una stringa base 64 
    public String recBase64 = null;
    
   
    public class Tracciamento {

        public Timestamp Data;
        public double Lat;
        public double Long;
        public int FuelLevel;
        public int StatoZB;
        public int ValidGPS = -1;
        public int QualityGPS = -1;
    }

    public class Evento {

        /* TypeEv: 0 - SOS
         *          1 - ACC lieve
         *          2 - ACC Grave
         *          6 - Main Supply Off
         *          7 - Main Supply On
         */
        public int TypeEv;
        public String EventDescr;

        public Evento() {
        }
        public Tracciamento T = new Tracciamento();
        public byte[] Extra = new byte[10];

        public String getString() {
            SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            String ret = "";
            ret += IDBlackBox + ",";
            ret += 0 + ",";
            ret += T.Lat + ",";
            ret += T.Long + ",";
            ret += FullDateFormat.format(T.Data) + ",";
            ret += TypeEv + ",";
            ret += new String(Hex.encode(Extra));

            return ret;
        }
    }

    public class TypeRecordRT {

        public int StatoZB = -1;
        public int Fuel = -1;
        public int NumTracciamenti;
        public Tracciamento T[] = new Tracciamento[3];

        public String getString() {
            SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            String ret = "";
            ret += IDBlackBox + ",";
            ret += 0 + ",";
            ret += T[0].Lat + ",";
            ret += T[0].Long + ",";
            ret += FullDateFormat.format(T[0].Data) + ",";
            ret += 0 + ",";
            ret += 0 + ",";
            ret += 0 + ",";
            ret += 0 + ",";
            ret += T[0].StatoZB + ",";
            ret += T[0].ValidGPS;


            return ret;
        }
    }

    public class TypeRecordZ {
    }

    public class TypeRecordY {
    }

    public class TypeRecordX {
    }

    public class TypeRecordI {

        public int ErrorNumber;
        public int MaiorVersion;
        public int MinorVersion;
        public String DateVersion = "";
        public String TimeVersion = "";
        public long LastOperativeTime;
        public int HWRev;
        public int NFC_MaiorVersion;
        public int NFC_MinorVersion;
        Timestamp BTimeStamp;
        public int PKRandID;
    }

    public class TypeRecordI2 {

        public String IMEI = "";
        public String ICCID = "";
        public int PKRandID;
    }

    public class TypeRecordI1 {

        public boolean[] Tags = new boolean[8];
        public int[] Soglia = new int[2];
        public String NumTelMaster = "";
        public String[] NumTelSlave = new String[7];
        public int NumTelConfig = 255;
        public long RandAuth;
        public int PkNum;
        public int PKRandID;
    }

    public class TypeRecordT {
    }

    public class TypeRecordE {

        public int NumEventi;
        public Evento E[] = new Evento[2];
    }

    public enum RecordTypes {

        RecordRT, RecordZ, RecordY, RecordX, RecordI, RecordT, RecordE
    };
    public int RecordSubType = 0;
    RecordTypes RecordType;

    public RecordTypes getRecordType() {
        return RecordType;
    }
    public TypeRecordRT RecRT;
    public TypeRecordE RecE;
    public TypeRecordI RecI;
    public TypeRecordI2 RecI2;
    public TypeRecordI1 RecI1;

    public void ParseRecord(long IDRec, long IDBlackBox, byte Rec[], Timestamp TS, boolean VerifyCRC)
            throws ExceptionInvalidRecordLenght, ExceptionCRCError, Exception {
        this.IDBlackBox = IDBlackBox;
        this.IDRec = IDRec;
        this.RecordReceivingTime = TS;
        
        
        
        String base64Encoded = DatatypeConverter.printBase64Binary(Rec);

        //salvo il contenuto
        this.recBase64 = base64Encoded;
        //System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+this.recBase64);

        byte Type = (byte) (Rec[0] >> 4);
        if (Rec.length < 48) {
            throw new ExceptionInvalidRecordLenght("Record too short");
        }
        // verifica del crc
        MessageDigest cript;

        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(Rec, 0, 47);
        } catch (NoSuchAlgorithmException ex) {
            throw new Exception("SHA1 Failure");
        }

        byte[] CRC = cript.digest();
        if (CRC[0] != Rec[47] && VerifyCRC) {
            //SAVERIO
            //throw new ExceptionCRCError("Errore sulla verifica del CRC");
        }
        //System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+Type+"<<<<<<<");
        if (Type == 4) { //record RealTime
            RecordType = RecordTypes.RecordRT;
            ParseRecordRT(Rec);
        } else if (Type == 3) {       //  Record Evento
            RecordType = RecordTypes.RecordE;
            ParseRecordE(Rec);
        } else if (Type == 6) {       //  Record Info
            RecordType = RecordTypes.RecordI;
            ParseRecordI(Rec);
        } else if (Type == 2) {
            this.RecordType = RecordTypes.RecordY;
        } else if (Type == 1) {
            this.RecordType = RecordTypes.RecordX;
        } else if (Type == 0) {
            this.RecordType = RecordTypes.RecordZ;
        }
    }

    private void ParseRecordRT(byte Rec[]) {
        RecRT = new TypeRecordRT();
        RecRT.T[0] = new Tracciamento();
        RecRT.T[0].StatoZB = Utils.uBToI(Rec[2]);
        RecRT.T[0].ValidGPS = Utils.uBToI(Rec[3]) & 0x1;
        RecRT.T[0].QualityGPS = Utils.uBToI(Rec[40]);

        byte B[] = new byte[8];
        System.arraycopy(Rec, 4, B, 0, 4);
        Timestamp TS = GetTimeStamp(B);
        if (TS == null) {
            TS = RecordReceivingTime;
        }
        if (TS != null) {
            //System.out.print("Timestamp=" + TS.toString());
        }
        RecRT.T[0].Data = TS;
        //Lat - Long
        System.arraycopy(Rec, 8, B, 0, 4);
        RecRT.T[0].Lat = GetCoord(B);
        System.arraycopy(Rec, 12, B, 0, 4);
        RecRT.T[0].Long = GetCoord(B);
        RecRT.Fuel = Utils.uBToI(Rec[1]);
        RecRT.NumTracciamenti = 1;
        //System.out.println(" Lat=" + RecRT.T[0].Lat + " Long=" + RecRT.T[0].Long + " Fuel=" + RecRT.Fuel);
    }

    public static double GetCoord(byte Data[]) {
        double Coord;
        int Sign = (Utils.uBToI(Data[1]) >> 7);
        int Gradi = Utils.uBToI(Data[0]);
//        if ((Utils.uBToI(Data[1]) >> 7) == 0) {
//            Gradi = -Gradi;
//        }
        int Dec = ((Utils.uBToI(Data[1]) & 0xF) << 16)
                + ((Utils.uBToI(Data[2]) & 0xFF) << 8)
                + ((Utils.uBToI(Data[3]) & 0xFF));
        
        if (Dec>0xF0000){
            Dec=0xFFFFF-Dec;
        }

        Coord = Gradi + (((double) Dec) / 10000);

        if (Sign == 0) {
            Coord = -Coord;
        }
/*
        System.out.println("\n\r val "+String.format("0x%2s", Integer.toHexString(Utils.uBToI(Data[0]))).replace(' ', '0') +
                " "+String.format("0x%2s", Integer.toHexString(Utils.uBToI(Data[1]))).replace(' ', '0')+
                " "+String.format("0x%2s", Integer.toHexString(Utils.uBToI(Data[2]))).replace(' ', '0')+
                " "+String.format("0x%2s", Integer.toHexString(Utils.uBToI(Data[3]))).replace(' ', '0')+
                "     Coord "+Coord+ "    Gradi "+Gradi+" Dec "+Dec);
*/
        return Coord;
    }

    public static Timestamp GetTimeStamp(byte Data[]) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        Calendar c = Calendar.getInstance(tz);

        Timestamp TS;
        int Year = (Utils.uBToI(Data[0]) >> 1) + 2000;
        int Month = ((Utils.uBToI(Data[0]) & 0x1) << 3) + ((Utils.uBToI(Data[1]) >> 5) & 0x7);
        int Day = ((Utils.uBToI(Data[1])) & 0x1F);
        int Hour = (Utils.uBToI(Data[2]) >> 3) & 0x1F;
        int Minutes = ((Utils.uBToI(Data[2]) & 0x7) << 3) + ((Utils.uBToI(Data[3]) >> 5) & 0x7);
        int Seconds = ((Utils.uBToI(Data[3])) & 0x1F) << 1;
        c.set(Year, Month - 1, Day, Hour, Minutes, Seconds);
        TS = new Timestamp(c.getTimeInMillis());
        
        /*
        LogsClass Log  = LogsClass.getInstance();
        Log.WriteLog(1, "TS :"+TS);
        Log.WriteLog(1, "TS.getYear() :"+TS.getYear());
        Log.WriteLog(1, "TS.getYear() + 1900 :"+(TS.getYear() + 1900 ));
        */
        
        if (TS.getYear() + 1900 < 1971) {
            TS = null;
        } else if (TS.getYear() + 1900 > 2030) {
            TS = null;
        }

        return TS;
    }

    private void ParseRecordE(byte Rec[]) {
        RecE = new TypeRecordE();
        RecE.E = new Evento[2];
        RecE.NumEventi = 1;
        RecE.E[0] = new Evento();
        RecE.E[0].T = new Tracciamento();

        RecE.E[0].TypeEv = Utils.uBToI(Rec[1]);
//                    Log.WriteLog(3,"ZB="+rs.getString("BBserial") +" IDRecord ="+rs.getLong(4)+" Rec Ev Type "+TypeEv+" ");
        byte B[] = new byte[8];
        System.arraycopy(Rec, 2, B, 0, 4);
        Timestamp TS = GetTimeStamp(B);
        if (TS == null) {
            TS = RecordReceivingTime;
        }
        if (TS != null) {
            //System.out.print("Timestamp=" + TS.toString());
        }
        RecE.E[0].T.Data = TS;
        System.arraycopy(Rec, 6, B, 0, 4);
        RecE.E[0].T.Lat = GetCoord(B);
        System.arraycopy(Rec, 10, B, 0, 4);
        RecE.E[0].T.Long = GetCoord(B);
        System.arraycopy(Rec, 14, RecE.E[0].Extra, 0, 10);
        if (RecE.E[0].TypeEv == 0) {
            RecE.E[0].EventDescr = "Distress";
        } else if (RecE.E[0].TypeEv == 1) {
            RecE.E[0].EventDescr = "Accelerometrico Lieve";
        } else if (RecE.E[0].TypeEv == 2) {
            RecE.E[0].EventDescr = "Accelerometrico Grave";
        } else if (RecE.E[0].TypeEv == 3) {
            RecE.E[0].EventDescr = "Tamper";
        } else if (RecE.E[0].TypeEv == 4) {
            RecE.E[0].EventDescr = "Sicurezza";
        } else if (RecE.E[0].TypeEv == 5) {
            RecE.E[0].EventDescr = "Aux";
        } else if (RecE.E[0].TypeEv == 6) {
            RecE.E[0].EventDescr = "Assenza Alimentazione Primaria";
        } else if (RecE.E[0].TypeEv == 7) {
            RecE.E[0].EventDescr = "Ripristino Alimentazione Primaria";
        } else if (RecE.E[0].TypeEv == 8) {
            RecE.E[0].EventDescr = "Warning Superamento Velocità";
        } else if (RecE.E[0].TypeEv == 9) {
            RecE.E[0].EventDescr = "Warning Superamento Distanza";
        } else if (RecE.E[0].TypeEv == 10) {
            RecE.E[0].EventDescr = "Accensione del dispositivo";
        } else if (RecE.E[0].TypeEv == 11) {
            RecE.E[0].EventDescr = "Spegnimento del dispositivo";
        } else if (RecE.E[0].TypeEv == 12) {
            RecE.E[0].EventDescr = "Autenticazione Conducente";
        } else if (RecE.E[0].TypeEv == 13) {
            RecE.E[0].EventDescr = "Spostamento a motore spento";
        } else if (RecE.E[0].TypeEv == 20) {
            RecE.E[0].EventDescr = "Eventi VDO";
        } else if (RecE.E[0].TypeEv == 21) {
            RecE.E[0].EventDescr = "Stato processo di calibrazione";
        } else if (RecE.E[0].TypeEv == 22) {
            RecE.E[0].EventDescr = "Evento batteria";
        }
//        System.out.print( RecE.E[0].EventDescr);

    }

    private void ParseRecordI(byte Rec[]) {
        RecordSubType = (Rec[41] >> 4) & 0x0F;
        if (RecordSubType == 0 || (RecordSubType == 2 && Rec[43] == 1)) {         // ZB info vecchio tipo
            RecI = new TypeRecordI();
            try {
                byte B[] = new byte[8];
                int i;

                RecI.ErrorNumber = (Utils.uBToI(Rec[1]) & 0xFF);
                RecI.MaiorVersion = (Utils.uBToI(Rec[2]) & 0xFF);
                RecI.MinorVersion = (Utils.uBToI(Rec[3]) & 0xFF);
                RecI.LastOperativeTime = ((Utils.uBToI(Rec[7]) & 0xFF) << 24)
                        + ((Utils.uBToI(Rec[6]) & 0xFF) << 16)
                        + ((Utils.uBToI(Rec[5]) & 0xFF) << 8)
                        + +(Utils.uBToI(Rec[4]) & 0xFF);

                RecI.HWRev              = (Utils.uBToI(Rec[31]) & 0xFF);
                RecI.NFC_MaiorVersion   = (Utils.uBToI(Rec[32]) & 0xFF);
                RecI.NFC_MinorVersion   = (Utils.uBToI(Rec[33]) & 0xFF);
                for (i = 0; i < 11; i++) {
                    RecI.DateVersion += (char) Rec[12 + i];
                }
                for (i = 0; i < 8; i++) {
                    RecI.TimeVersion += (char) Rec[23 + i];
                }

                
                System.arraycopy(Rec, 8, B, 0, 4);
                Timestamp BTime = GetTimeStamp(B);
                if (BTime == null) {
                    BTime = new java.sql.Timestamp((new java.util.Date()).getTime());
                }
                if (BTime != null) {
                    //System.out.print("Timestamp=" + BTime.toString());
                }
                RecI.BTimeStamp = BTime;
                RecI.PKRandID = ((Utils.uBToI(Rec[2 + 44]) & 0xFF) << 16)
                        + ((Utils.uBToI(Rec[1 + 44]) & 0xFF) << 8)
                        + ((Utils.uBToI(Rec[0 + 44]) & 0xFF));

            } catch (Exception ex) {
                LogsClass.getInstance().WriteEx(DBAdminClass.class.getName(), ex);
                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (RecordSubType == 2 && Rec[43] == 0) {
            RecI2 = new TypeRecordI2();
            int i;
            RecI2.ICCID = "";
            RecI2.IMEI = "";
            for (i = 0; i < 15; i++) {
                RecI2.IMEI += (char) Rec[0 + i + 1];
            }
            for (i = 0; i < 24; i++) {
                RecI2.ICCID += (char) Rec[15 + i + 1];
            }
            RecI2.PKRandID = ((Utils.uBToI(Rec[2 + 44]) & 0xFF) << 16)
                    + ((Utils.uBToI(Rec[1 + 44]) & 0xFF) << 8)
                    + ((Utils.uBToI(Rec[0 + 44]) & 0xFF));
        } else if (RecordSubType == 1 && Rec[43] == 1) {
            RecI1 = new TypeRecordI1();
            RecI1.PkNum = 0;
            int i;
//            RecI1.Tags=new boolean[8];
            for (i = 0; i < 8; i++) {
                RecI1.Tags[i] = false;
            }

            String Tags = "";
            for (i = 0; i < 8; i++) {
                Tags += (char) Rec[1 + i];
            }

            RecI1.Tags[0] = Tags.contains("D");
            RecI1.Tags[1] = Tags.contains("T");
            RecI1.Tags[2] = Tags.contains("S");
            RecI1.Tags[3] = Tags.contains("A");
            RecI1.Tags[4] = Tags.contains("B");
            RecI1.Tags[5] = Tags.contains("W");
            RecI1.Tags[6] = Tags.contains("H");
            RecI1.Tags[7] = Tags.contains("E");

            RecI1.Soglia[0] = ((Utils.uBToI(Rec[4 + 8]) & 0xFF) << 24)
                    + ((Utils.uBToI(Rec[3 + 8]) & 0xFF) << 16)
                    + ((Utils.uBToI(Rec[2 + 8]) & 0xFF) << 8)
                    + ((Utils.uBToI(Rec[1 + 8]) & 0xFF));
            RecI1.Soglia[1] = ((Utils.uBToI(Rec[8 + 8]) & 0xFF) << 24)
                    + ((Utils.uBToI(Rec[7 + 8]) & 0xFF) << 16)
                    + ((Utils.uBToI(Rec[6 + 8]) & 0xFF) << 8)
                    + ((Utils.uBToI(Rec[5 + 8]) & 0xFF));

            byte Buff[] = new byte[7];
            java.lang.System.arraycopy(Rec, 17, Buff, 0, 7);
            RecI1.NumTelMaster = Utils.toTelNumber(Buff);

            java.lang.System.arraycopy(Rec, 24, Buff, 0, 7);
            RecI1.NumTelSlave[0] = Utils.toTelNumber(Buff);
            java.lang.System.arraycopy(Rec, 31, Buff, 0, 7);
            RecI1.NumTelSlave[1] = Utils.toTelNumber(Buff);

            RecI1.PKRandID = ((Utils.uBToI(Rec[2 + 44]) & 0xFF) << 16)
                    + ((Utils.uBToI(Rec[1 + 44]) & 0xFF) << 8)
                    + ((Utils.uBToI(Rec[0 + 44]) & 0xFF));

        } else if (RecordSubType == 1 && Rec[43] == 0) {
            RecI1 = new TypeRecordI1();
            RecI1.PkNum = 1;
            int i;
            byte Buff[] = new byte[7];
            for (i = 0; i < 5; i++) {
                java.lang.System.arraycopy(Rec, 1 + i * 7, Buff, 0, 7);
                RecI1.NumTelSlave[i + 2] = Utils.toTelNumber(Buff);
            }
            RecI1.RandAuth = ((Utils.uBToL(Rec[3 + 36]) & 0xFF) << 24)
                    + ((Utils.uBToL(Rec[2 + 36]) & 0xFF) << 16)
                    + ((Utils.uBToL(Rec[1 + 36]) & 0xFF) << 8)
                    + ((Utils.uBToL(Rec[0 + 36]) & 0xFF));
            RecI1.NumTelConfig = Utils.uBToI(Rec[0 + 40]);
//            RecI1.RandAuth= ((Utils.uBToL(Rec[2+36])&0xFF)<<16)+
//                            ((Utils.uBToL(Rec[1+36])&0xFF)<<8)+
//                            ((Utils.uBToL(Rec[0+36])&0xFF));

            RecI1.PKRandID = ((Utils.uBToI(Rec[2 + 44]) & 0xFF) << 16)
                    + ((Utils.uBToI(Rec[1 + 44]) & 0xFF) << 8)
                    + ((Utils.uBToI(Rec[0 + 44]) & 0xFF));

        }

    }
}
