/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import ZB_DataAnalisysPack.ZB_DataAnalisys;
import ZB_DataArchivingPack.ZB_DataArchiving;
import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.zbox;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class ZB_ElaboraDati extends Thread {

    DBAdminClass DBAdmin;
    ConfClass Conf;
    LogsClass Log;
    ZB_SWUpdateProcess ZB_SWUpdate;
    ZB_DataAnalisys ZB_DataAnalizer;
    ZB_DataArchiving ZB_DataArchiver;
    ZB_ReservationProcess ZB_Reservation;
    ZB_TestProcess ZB_Test;
//    static int ProcessCount=1;
    static int ProcessCount = 200;
    static int ExecutorCount = 20;
    ExecutorService executor;
    int RunningProcess = 0;
    ArrayList<ZB_Record_Parser> ZB_R_P;
    Timestamp StartTime;

    public ZB_ElaboraDati() {
        if (executor == null) {
            executor = Executors.newFixedThreadPool(ExecutorCount);
        }
        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
    }

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin) {
        DBAdmin = LDBAdmin;

        return true;
    }

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin, ConfClass LConf, LogsClass LLog) {
        DBAdmin = LDBAdmin;
        Conf = LConf;
        Log = LLog;
        return true;
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {
        ZB_SWUpdate = new ZB_SWUpdateProcess();
        ZB_SWUpdate.SetEnvironmentClasses(DBAdmin, Conf, Log);

        ZB_DataAnalizer = new ZB_DataAnalisys();
        ZB_DataAnalizer.SetEnvironmentClasses(DBAdmin, Conf, Log);

        ZB_DataArchiver = new ZB_DataArchiving();
        ZB_DataArchiver.SetEnvironmentClasses(DBAdmin, Conf, Log);

        ZB_Reservation = new ZB_ReservationProcess();
        ZB_Reservation.SetEnvironmentClasses(DBAdmin, Conf, Log);

        ZB_Test = new ZB_TestProcess();
        ZB_Test.SetEnvironmentClasses(DBAdmin);

        if (Conf.ParseRecord == 0) {
            Log.WriteLog(1, "Elaboratore dei record NON attivato");
        }

        if (Conf.CertidriveCheckData == 0) {
            Log.WriteLog(1, "Elaboratore dei dati Certidrive NON attivato");
        }

        if (Conf.SWUpdateProcess == 0) {
            Log.WriteLog(1, "Elaboratore dei SW update NON attivato");

        }

        if (Conf.CheckReservationProcess == 0) {
            Log.WriteLog(1, "Elaboratore delle Prenotazioni (Reservation) NON attivato");
        }

        if (Conf.ZBTestProcess == 1) {
            ZB_Test.RunZBTestProcess();
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

        while (true) {
            RunningProcess = 0;
            StartTime = new java.sql.Timestamp((new java.util.Date()).getTime());
            try {
                if (Conf.ParseRecord != 0) {
//                    DBAdmin.ReadRecord();
                    ParserInit();
                    WaitExecutionComplete();
                }

                if (Conf.CertidriveCheckData != 0) {
                    DBAdmin.CertidriveCheckChanges();
                }
//                if (Conf.CertidriveCheckData!=0) { DBAdmin.CertidriveCheckRecordsZXY_old();  }
                if (Conf.CertidriveCheckRecords != 0) {
                    DBAdmin.CertidriveCheckRecordsZXY();
                }

                if (Conf.CheckPushNotify != 0) {
                    PushNotifyThread PNT = new PushNotifyThread();
                    PNT.CheckPushNotifyEvents(DBAdmin);
                }

                if (Conf.SWUpdateProcess != 0) {
                    ZB_SWUpdate.SWUpdateStatusCheck();
                }

//                if (Conf.DataAnalizerProcess!=0) { ZB_DataAnalizer.ZBDataRunCheck(); }
                if (Conf.DataAnalizerProcess != 0) {
                    ZB_DataAnalizer.RipristinaSalti_connbbgruppoerg();
                    ZB_DataAnalizer.RipristinaSalti_connbbCOAP();
                    ZB_DataAnalizer.RipristinaSalti_connbbCOAP_production();
                    ZB_DataAnalizer.RipristinaSalti_connbbCONAP();
                    ZB_DataAnalizer.RipristinaSalti_connbbCONAP_production();
                    ZB_DataAnalizer.RipristinaSalti_connbbALD();
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbisacarpooling");  // specifica per CarPooling
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbLML");  // specifica per LML / TPMS
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbtestani");  // specifica per Testani / TPMS
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbciampino");  // specifica per Ciampino
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbtilab");  // specifica per Tilab

                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbFCS");  // specifica per Full Car Service
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbtelecomcarpooling");  // specifica per CarPooling
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbacicarsharing");  // specifica per ACI CarSharing
                    ZB_DataAnalizer.RipristinaSalti_connbb("connbbisacarpooling");  // specifica per ACI CarSharing
                    ZB_DataAnalizer.RipristinaSalti();              // Salti TelecomItalia
                }

                if (Conf.DataArchiver != 0) {
                    ZB_DataArchiver.ZBDataRunArchiving();
                }

                if (Conf.CheckReservationProcess != 0) {
                    ZB_Reservation.CheckReservation_ZB_Stato();
                }

                if (Conf.ZBTestProcess == 1) {
                    ZB_Test.RunZBTestProcess();
                }



                Thread.sleep(10);
//                Thread.sleep(1);
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_ElaboraDati.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private int ParserInit() {
        ArrayList<ArrayList<zbox>> ZBGroup;
        ZB_R_P = new ArrayList<ZB_Record_Parser>();
        // prende la lista delle ZBox da trattare
        ZBGroup = ZB_Record_Parser.GetZBoxListGroup(DBAdmin.DbAdminConn, ProcessCount);

        // attiva tutti gli esecutori
        for (int i = 0; i < ZBGroup.size(); i++) {
            ZB_Record_Parser Proc = new ZB_Record_Parser(ZBGroup.get(i));
            try {
                sleep(200L);
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_ElaboraDati.class.getName()).log(Level.SEVERE, null, ex);
            }
            ZB_R_P.add(Proc);
            executor.execute(Proc);
            RunningProcess++;
        }
        return 0;
    }

    private int WaitExecutionComplete() {
        int Errors;
        Timestamp LastCheck = new java.sql.Timestamp((new java.util.Date()).getTime());
        Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
        float TimeDiff = ((float) (now.getTime() - StartTime.getTime())) / 1000;

        while (RunningProcess != 0) {
            Errors = 0;
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                Logger.getLogger(ZB_ElaboraDati.class.getName()).log(Level.SEVERE, null, ex);
                return -1;
            }
            RunningProcess = 0;
            for (int i = 0; i < ZB_R_P.size(); i++) {
                if (ZB_R_P.get(i) != null) {
                    if (ZB_R_P.get(i).Running) {
                        RunningProcess++;
                    }
                    Errors += ZB_R_P.get(i).Errors;
                }
            }

            now = new java.sql.Timestamp((new java.util.Date()).getTime());
            TimeDiff = ((float) (now.getTime() - StartTime.getTime())) / 1000;

            if (TimeDiff > 800) {
                System.out.println("Terminazione non corretta di uno dei processi");
                executor.shutdown();
                for (int i = 0; i < ZB_R_P.size(); i++) {
                    if (ZB_R_P.get(i) != null) {
                        if (ZB_R_P.get(i).isAlive()) {
                            ZB_R_P.get(i).interrupt();
                        }
                    }
                }

                return -2;
            }

            float TimeDiffLastCheck = ((float) (now.getTime() - LastCheck.getTime())) / 1000;
            if (TimeDiffLastCheck > 30) {
                LastCheck = new java.sql.Timestamp((new java.util.Date()).getTime());
                Log.WriteLog(3, "Processo in esecuzione da " + TimeDiff + " sec(RunningProcess=" + RunningProcess + ")");
            }
        }

        return 0;
    }
}
