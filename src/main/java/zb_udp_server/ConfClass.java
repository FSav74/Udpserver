/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.zbox.zbox;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//import java.util.logging.Level;
//import java.util.logging.Logger;
/**
 *
 * @author Luca
 */
public class ConfClass {

    public String Version = "ZBox UDP Server v0.1";
    public String userName = "CABlackBox";
    public String password = "3rYXMcEENRPLHuSK";
    public String url = "jdbc:mysql://192.168.1.237/";
    public String AdminDBName = "blackbox_debug";
    public String UserNameTaxiDB = "CABlackBox";
    public String PasswordTaxiDB = "3rYXMcEENRPLHuSK";
    public String UrlTaxiDB = "jdbc:mysql://192.168.1.246/";
    public String TaxiDBName = "taxi";
    public String LogPath = "C:\\DT\\";
    public int LogLevel = 1;
    public int Vlim1 = 0;
    public int Vlim2 = 50;
    public int Vlim3 = 90;
    public int Vlim4 = 130;
    public boolean debug = false;
    public String ComPort = "COM2";
    public int ParseRecord = 0;
    public int SWUpdateProcess = 0;
    public int DataAnalizerProcess = 0;
    public int CertidriveCheckData = 0;
    public int CertidriveCheckRecords = 0;
    public int CheckPushNotify = 0;
    public int ZBTestProcess = 0;
    public int DataArchiver = 0;
    public int ListenPort = 8802;
    public int ActiveSessionLimit = 8000;
    public int MaxUpdateSession = 60;
    public int TaxiTCPConn = 0;
    public int TaxiDataCheck = 0;
    public String TaxiServerIP = "";
    public int TaxiServerPort = 0;
    public int CheckReservationProcess = 0;       // abilita il processo di verifica delle prenotazioni per badge RFID/NFC
    public int DisableDBWriteData = 0;
    public String CertidriveUserName = "";
    public String CertidrivePassword = "";
    public String CertidriveUrl = "";
    public String CertidriveAdminDBName = "cddb_viacom_adm";
    public String FileNameLocalization = "ZBLocalization.txt";
    public String FileNameEvents = "ZBEvents.txt";
    public List<zbox> CacheZBox = Collections.synchronizedList(new ArrayList<zbox>());
    private LogsClass Log;
    private static ConfClass instance = null;
    
    public String kafkaTopicName = "";

    public static ConfClass getInstance() {
        if (instance == null) {
            instance = new ConfClass();
        }
        return instance;
    }

    public ConfClass() {
        try {
            if (isWindows()) {
                System.out.println("This is Windows");
            } else if (isMac()) {
                System.out.println("This is Mac");
                LogPath = "./log/";
            } else if (isUnix()) {
                System.out.println("This is Unix or Linux");
                LogPath = "./log/";
            } else if (isSolaris()) {
                System.out.println("This is Solaris");
                LogPath = "./log/";
            } else {
                System.out.println("Your OS is not support!!");
            }
        } finally {
//           Log=LogsClass.getInstance();
        }
    }

    /**
     * La funzione legge i parametri di configurazione dal Config file passato
     * come parametro
     *
     * @param FileName
     */
    public boolean Init(String FileName) {
        //Log = LogsClass.getInstance();
        File PConfigFile = new File(FileName);
//        ArrayList<ConfMonitorDirClass> CFList = new ArrayList<ConfMonitorDirClass>();
        if (isWindows()) {
            System.out.println("This is Windows");
        } else if (isMac()) {
            System.out.println("This is Mac");
            LogPath = "./log/";
        } else if (isUnix()) {
            System.out.println("This is Unix or Linux");
            LogPath = "./log/";
        } else if (isSolaris()) {
            System.out.println("This is Solaris");
            LogPath = "./log/";
        } else {
            System.out.println("Your OS is not support!!");
        }

        if (PConfigFile.isFile()) {
            try {
                FileInputStream BlackBoxIDFile;

                BlackBoxIDFile = new FileInputStream(PConfigFile);

                BufferedReader dis = new BufferedReader(new InputStreamReader(BlackBoxIDFile));
                String line;
                while ((line = dis.readLine()) != null) {
                    int i = line.indexOf('!');

                    if (i >= 0) {
                        line = line.substring(0, i);
                    }
                    if (line.contains("ZBModemInterface.UserName=")) {
                        userName = line.substring(("ZBModemInterface.UserName=").length());
                    } else if (line.contains("ZBModemInterface.password=")) {
                        password = line.substring(("ZBModemInterface.password=").length());
                    } else if (line.contains("ZBModemInterface.DBServerIP=")) {
                        String IP = line.substring(("ZBModemInterface.DBServerIP=").length());
                        url = "jdbc:mysql://" + IP + "/";
                    } else if (line.contains("ZBModemInterface.AdminDBName=")) {
                        AdminDBName = line.substring(("ZBModemInterface.AdminDBName=").length());
                    } else if (line.contains("ZBModemInterface.LogPath=")) {
                        LogPath = line.substring(("ZBModemInterface.LogPath=").length());
                    } else if (line.contains("ZBModemInterface.LogLevel=")) {
                        LogLevel = Integer.parseInt(line.substring(("ZBModemInterface.LogLevel=").length()));
                    } else if (line.contains("ZBModemInterface.Debug")) {
                        debug = true;
                    } else if (line.contains("ZBModemInterface.ComPort=")) {
                        ComPort = line.substring(("ZBModemInterface.ComPort=").length());
                    } else if (line.contains("ZBModemInterface.ParseRecord=")) {
                        ParseRecord = Integer.parseInt(line.substring(("ZBModemInterface.ParseRecord=").length()));
                    } else if (line.contains("ZBModemInterface.CertidriveCheckData=")) {
                        CertidriveCheckData = Integer.parseInt(line.substring(("ZBModemInterface.CertidriveCheckData=").length()));
                    } else if (line.contains("ZBModemInterface.CertidriveCheckRecords=")) {
                        CertidriveCheckRecords = Integer.parseInt(line.substring(("ZBModemInterface.CertidriveCheckRecords=").length()));
                    } else if (line.contains("ZBModemInterface.CheckPushNotify=")) {
                        CheckPushNotify = Integer.parseInt(line.substring(("ZBModemInterface.CheckPushNotify=").length()));
                    } else if (line.contains("ZBModemInterface.ListenPort=")) {
                        ListenPort = Integer.parseInt(line.substring(("ZBModemInterface.ListenPort=").length()));
                    } else if (line.contains("ZBModemInterface.SWUpdateProcess=")) {
                        SWUpdateProcess = Integer.parseInt(line.substring(("ZBModemInterface.SWUpdateProcess=").length()));
                    } else if (line.contains("ZBModemInterface.DataAnalizerProcess=")) {
                        DataAnalizerProcess = Integer.parseInt(line.substring(("ZBModemInterface.DataAnalizerProcess=").length()));
                    } else if (line.contains("ZBModemInterface.DataArchiver=")) {
                        DataArchiver = Integer.parseInt(line.substring(("ZBModemInterface.DataArchiver=").length()));
                    } else if (line.contains("ZBModemInterface.ActiveSessionLimit=")) {
                        ActiveSessionLimit = Integer.parseInt(line.substring(("ZBModemInterface.ActiveSessionLimit=").length()));
                    } else if (line.contains("ZBModemInterface.MaxUpdateSession=")) {
                        MaxUpdateSession = Integer.parseInt(line.substring(("ZBModemInterface.MaxUpdateSession=").length()));
                    } else if (line.contains("ZBModemInterface.TaxiTCPConn=")) {    // abilita il modulo di trasmissione per Taxi
                        TaxiTCPConn = Integer.parseInt(line.substring(("ZBModemInterface.TaxiTCPConn=").length()));
                    } else if (line.contains("ZBModemInterface.TaxiDataCheck=")) {    // abilita il modulo di trasmissione per Taxi
                        TaxiDataCheck = Integer.parseInt(line.substring(("ZBModemInterface.TaxiDataCheck=").length()));
                    } else if (line.contains("ZBModemInterface.TaxiServerIP=")) {    // abilita il modulo di trasmissione per Taxi
                        TaxiServerIP = line.substring(("ZBModemInterface.TaxiServerIP=").length());
                    } else if (line.contains("ZBModemInterface.TaxiServerPort=")) {    // abilita il modulo di trasmissione per Taxi
                        TaxiServerPort = Integer.parseInt(line.substring(("ZBModemInterface.TaxiServerPort=").length()));
                    } else if (line.contains("ZBModemInterface.DisableDBWriteData=")) {    // Disabilita la scrittura dei dati du DB
                        DisableDBWriteData = Integer.parseInt(line.substring(("ZBModemInterface.DisableDBWriteData=").length()));
                    } else if (line.contains("ZBModemInterface.CheckReservationProcess=")) {    // abilita il processo di verifica delle prenotazioni per badge RFID/NFC
                        CheckReservationProcess = Integer.parseInt(line.substring(("ZBModemInterface.CheckReservationProcess=").length()));
                    } else if (line.contains("ZBModemInterface.ZBTestProcess=")) {    // abilita il processo di Test sui dispositivi
                        ZBTestProcess = Integer.parseInt(line.substring(("ZBModemInterface.ZBTestProcess=").length()));
                    } // CERTIDRIVE WEB
                    else if (line.contains("CertidriveWeb.UserName=")) {
                        CertidriveUserName = line.substring(("CertidriveWeb.UserName=").length());
                    } else if (line.contains("CertidriveWeb.Password=")) {
                        CertidrivePassword = line.substring(("CertidriveWeb.Password=").length());
                    } else if (line.contains("CertidriveWeb.DBServerIP=")) {
                        String IP = line.substring(("CertidriveWeb.DBServerIP=").length());
                        CertidriveUrl = "jdbc:mysql://" + IP + "/";
                    } else if (line.contains("CertidriveWeb.AdminDBName=")) {
                        CertidriveAdminDBName = line.substring(("CertidriveWeb.AdminDBName=").length());
                    }
                    
                    System.out.println(line);
                }
                BlackBoxIDFile.close();

            } catch (Exception ex) {
                Log.WriteEx(ConfClass.class.getName(), ex);
                Log.WriteLog(0, ex.getMessage());
                System.err.println("ERROR : " + ex.toString());
                return false;
            }
        } else {
            System.err.println("Invalid Config File: " + FileName);
        }
        return true;
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
    }

    public static boolean isWindows() {
        String os = System.getProperty("os.name").toLowerCase();
        // windows
        return (os.indexOf("win") >= 0);
    }

    public static boolean isMac() {
        String os = System.getProperty("os.name").toLowerCase();
        // Mac
        return (os.indexOf("mac") >= 0);
    }

    public static boolean isUnix() {
        String os = System.getProperty("os.name").toLowerCase();
        // linux or unix
        return (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0);
    }

    public static boolean isSolaris() {
        String os = System.getProperty("os.name").toLowerCase();
        // Solaris
        return (os.indexOf("sunos") >= 0);
    }
}
