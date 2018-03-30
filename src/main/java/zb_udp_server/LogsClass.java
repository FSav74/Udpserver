/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca Tricca
 */
public class LogsClass {

    ConfClass Conf;
    String LogPath;
    File LogFile;
    String Today;
    FileWriter FW;
    FileWriter FW_Localization;
    FileWriter FW_Events;
//    boolean debug= false;
    boolean debug = true;
    int LogLevel = 1;
    SimpleDateFormat DateFormat = new SimpleDateFormat("yyyyMMdd");
    SimpleDateFormat FullDateFormat = new SimpleDateFormat("[dd/MM/yyyy HH:mm:ss.SSSS] ");
    private static LogsClass instance = null;

    public LogsClass() {
        Conf = ConfClass.getInstance();
        Init(Conf.LogPath, Conf.debug, Conf.LogLevel);
    }

    public static LogsClass getInstance() {
        if (instance == null) {
            instance = new LogsClass();
        }
        return instance;
    }

    public boolean Init(String Path, boolean debug_status, int LLevel) {
        LogPath = Path;
        debug = debug_status;
        LogLevel = LLevel;

        if (!OpenLogFile()) {
            return false;
        }

        return true;
    }

    /**
     * Apre il file di log, eventualmente chiude il precedente
     *
     * @return true se apertura si è conclusa in modo corretto false altrimenti
     */
    private boolean OpenLogFile() {
        Today = DateFormat.format(new Date());

        DeInit();

        //LogFile = new File (LogPath+"ZBoxUDPServerLogDev_"+Today+".log"); //DEV LOG
        LogFile = new File(Conf.LogPath + "ZBoxUDPServerLog_" + Today + ".log");
        try {
            FW = new FileWriter(LogFile, true);
            WriteLog(0, "Application Start");
        } catch (IOException ex) {
            Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    /**
     * Scrive una segnalazione di log all'interno della log file
     *
     * @param LLevel : livello di log
     * @param Str : String a da accodare al log file
     */
    public void WriteLog(int LLevel, String Str) {
        CheckRotate();
        try {
            if (LLevel <= LogLevel) {
                Write(Str);
            }
        } catch (IOException ex) {
            Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Scrive una segnalazione di eccezione all'interno del log file
     *
     * @param ClassName : nome della classe che ha generato l'eccezione
     * @param ex1 : eccezione generata
     */
    public void WriteEx(String ClassName, Exception ex1) {
        CheckRotate();
        Logger.getLogger(ClassName).log(Level.SEVERE, null, ex1);
    }

    /**
     * *
     * provvede a ruotare il file di Log alla fine della giornata
     */
    private void CheckRotate() {
        if (Today == null) {
            OpenLogFile();
        } else if (!Today.equals(DateFormat.format(new Date()))) {
            OpenLogFile();
        }
    }

    /**
     * *
     * Esegue la scrittura effettiva nel log file
     *
     * @param Str
     */
    private void Write(String Str) throws IOException {
        String FullString = FullDateFormat.format(new Date()) + Str;
        FW.write(FullString + "\r\n");
        
       //per TEST: riabilito il flush
        FW.flush();
        if (debug) {
            System.out.println(FullString);
//            System.out.flush();

        }
    }

    /**
     * Chiude i file ed esegue flush delle code
     */
    public void DeInit() {
        try {
            Write("Chiusura Log file");
            FW.flush();
            FW.close();
            if (FW_Localization != null) {
                FW_Localization.flush();
                FW_Localization.close();
            }
            if (FW_Events != null) {
                FW_Events.flush();
                FW_Events.close();
            }

        } catch (IOException ex) {
            Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(LogsClass.class.getName()).log(Level.INFO, "No log log file present");
        }
    }

    /**
     * Accoda una localizzazione al file di Localization Se il file non è aperto
     * lo apre
     *
     * @param Str : String a da accodare al log file
     */
    public void WriteLocalization(float Lat, float Long) {
        if (FW_Localization == null) {     // file open
            File LocFile;
            LocFile = new File(Conf.LogPath + "ZBoxLocalization.log");
            try {
                FW_Localization = new FileWriter(LocFile, true);
            } catch (IOException ex) {
                Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
//        try {
//            FW_Localization.append(Str+"\r\n");
//        } catch (IOException ex) {
//            Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    /**
     * Accoda una localizzazione al file di Localization Se il file non è aperto
     * lo apre
     *
     * @param Str : String a da accodare al log file
     */
    public void WriteLocalization(String Str) {
        if (FW_Localization == null) {     // file open
            File LocFile;
            LocFile = new File(Conf.LogPath + "ZBoxLocalization.log");
            try {
                FW_Localization = new FileWriter(LocFile, true);
            } catch (IOException ex) {
                Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        try {
            FW_Localization.append(Str + "\r\n");
            FW_Localization.flush();
        } catch (IOException ex) {
            Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Accoda un Evento al file di Eventi Se il file non è aperto lo apre
     *
     * @param Str : String a da accodare al log file
     */
    public void WriteEvent(String Str) {
        if (FW_Events == null) {     // file open
            File EvFile;
            EvFile = new File(Conf.LogPath + "ZBoxEvent.log");
            try {
                FW_Events = new FileWriter(EvFile, true);
            } catch (IOException ex) {
                Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        try {
            FW_Events.append(Str + "\r\n");
            FW_Events.flush();
        } catch (IOException ex) {
            Logger.getLogger(LogsClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
