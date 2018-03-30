/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.Utils;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class ZB_UDP_Server {

    static Connection DbAdminConn = null;
    static String userName = "viacom_db_user";
    static String password = "viacom_db_user";
    static String url = "jdbc:mysql://192.168.1.3/blackbox_debug";
    static DBConnector DBConn = null;
    static DBAdminClass DBAdmin;
    static ConfClass Conf;
    static LogsClass Log;
//    static CommFrontEnd SessionCommFrontEnd;
//    static SerialModem SM = new SerialModem();
    static TCPConnect TCPConn = null;
    static TCPTaxiConnect TCPTaxiConn = null;
    static int StatoOP = 0;
    static int IDTrack = -1;
    static int IDSession = -1;
    static long CallTimeout = -1;
    static DatagramSocket serverSocket;
    
    //private static NotifyingBlockingThreadPoolExecutor threadPool = null;
    private static ExecutorService threadPool  = null;
    /**
     * @param args the command line arguments
     */
    @SuppressWarnings("SleepWhileInLoop")
    public static void main(String[] args) {
        // TODO code application logic here
        int Stato = 0;
        Timestamp StartTime = new Timestamp((new java.util.Date()).getTime());
        while (true) {
            Timestamp now = new Timestamp((new java.util.Date()).getTime());
//            if (now.getTime()>StartTime.getTime()+24*60*60*1000) {
           /* if (now.getTime() > StartTime.getTime() + 8 * 60 * 1000) {
                Log.WriteLog(4, " terminazione per trascorsi 8 minuti di esecuzione");
                System.exit(0);
                return;  // interrompe il modulo dopo 5 minuti di esecuzione
                // il sistema provvederà a farlo ripartire
            }*/                                                            // per evitare memory leak
            switch (Stato) {
                case 0:
                    //Conf = new ConfClass();
                    Conf = ConfClass.getInstance();
                    if (Conf.Init(args[0])) {
                        Stato++;
                    } else {
                        Stato = 40;
                    }
                    break;
                case 49:
                    Conf.DeInit();
                    Stato++;
                    break;
                case 1:
                    // inizializza il logger
                    //Log = new LogsClass();   //configurazione Log
                    Log = Log.getInstance();
                    if (Log.Init(Conf.LogPath, Conf.debug, Conf.LogLevel)) {
                        Stato++;
                    } else {
                        Stato = 48;
                    }
                    break;
                case 48:
                    Log.DeInit();
                    Stato++;
                    break;

                case 2:
                    //InitAdminDbConnection();
                    DBConn = DBConnector.getInstance();
                    DBConn.Init();

                    DBAdmin = new DBAdminClass();
                    DBAdmin.SetConf(Conf);
                    DBAdmin.SetLog(Log);
                    if (DBAdmin.Init()) {
                        Stato++;
                    } else {
                        Stato = 47;
                    }

                    // Attivo il server di comunicazione TCP
                    if (TCPConn != null) {
                        TCPConn.Close();
                    }
                    TCPConn = new TCPConnect();
                    TCPConn.SetConf(Conf);
                    TCPConn.SetLog(Log);
                    TCPConn.start();
                    if (Conf.TaxiTCPConn > 0) {
                        TCPTaxiConn = new TCPTaxiConnect();
                        TCPTaxiConn.start();
//                        TCPTaxiConn.TestCode();
                    }
                    //Stato ++;

                    break;
                case 47:
                    DBAdmin.DeInit();
                    TCPConn.Close();
                    Stato++;
                    break;
                case 3:
                    /*                    SessionCommFrontEnd = new CommFrontEnd();
                     SessionCommFrontEnd.SetConf(Conf);
                     SessionCommFrontEnd.SetLog(Log);
                     SessionCommFrontEnd.SetDBAdmin(DBAdmin);
                     SessionCommFrontEnd.StartConnection();*/
//                    if (!SM.isOpen())
//                        SM.OpenModem(Conf.ComPort);
                    Stato = 10;
                    break;
                case 10:   // stato Operativo
                    if (StatoOperativo()) {
                        Stato = 10;
                    } else {
                        Stato = 46;
                    }
                    break;
                case 46:
//                    SM.Close();
                    Stato++;
                    break;
                case 50:    // wait
                    try {
                        Thread.sleep(4000);
                    } catch (InterruptedException ex) {
                        Log.WriteEx(ZB_UDP_Server.class.getName(), ex);
                    }

                    Stato = 0;
                    break;
            }
        }
    }

    private static boolean StatoOperativo() {

        int Port = Conf.ListenPort;

        switch (StatoOP) {
            case 0:
                System.out.println("Starting ElaboraDati Poll Process X");
               
                //?????????????????????
                /*ZB_ElaboraDati ZB_El = new ZB_ElaboraDati();
                ZB_El.SetEnvironmentClasses(DBAdmin, Conf, Log);

                ZB_El.start();*/

                System.out.println("UDP Server running on port " + Port);
                try {
                    serverSocket = new DatagramSocket(Port);
                    serverSocket.setSoTimeout(5000);
                    StatoOP++;
                } catch (SocketException ex) {
                    Logger.getLogger(ZB_UDP_Server.class.getName()).log(Level.SEVERE, null, ex);
                    StatoOP = 50;
                }
                int THREAD_COUNT = 400;
                /*threadPool = new NotifyingBlockingThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT,3L, TimeUnit.SECONDS, 3L, TimeUnit.SECONDS,
                            new Callable<Boolean>() {
                               
                            public Boolean call() throws Exception {
                                LogsClass Log = null;
                                Log =Log.getInstance();
                                Log.WriteLog(1, "Rejected call........");
                                return false;
                            }});
                */
                
                threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
                
                break;
            case 1:
                try {
                    //ZB_UDP_Comunication1 ZB_Conn;
                    
                    
                    
                    //LimitedQueue<Runnable> blockingQueue = new LimitedQueue<Runnable>(5000);
                    //Executors.new(THREAD_COUNT,THREAD_COUNT, THREAD_COUNT,   TimeUnit.MILLISECONDS, blockingQueue);

                    //threadPool = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT,10000L, TimeUnit.MILLISECONDS,blockingQueue);

                    //ZB_Conn = new ZB_UDP_Comunication1();
//                    ZB_Conn.SetEnvironmentClasses(DBAdmin, Conf, Log);
                    //ZB_Conn.SetEnvironmentClasses(DBAdmin);
                    
                    
                    
                    byte[] receiveData = new byte[1024];
                    DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

                    

                    serverSocket.receive(receivePacket);
                    //ZB_Conn.SetConnection(serverSocket, receivePacket);
                      
                    threadPool.execute(new ZB_UDP_ComunicationNew(DBAdmin,serverSocket, receivePacket));
                    //ZB_Conn.start();
                    
                    
                 
                    

                    //(new ZB_UDP_Comunication(serverSocket,receivePacket)).start();
                } catch (SocketTimeoutException ex) {
//                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                      //Log.WriteLog(1, "TimeOut Exception1: "+ex.toString());
                } catch (SocketException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                    StatoOP = 50;
                    Log.WriteLog(1, "serverSocket Exception2: "+ex.toString());
                } catch (IOException ex) {
                    Logger.getLogger(ZB_UDP_ComunicationNew.class.getName()).log(Level.SEVERE, null, ex);
                    StatoOP = 50;
                    Log.WriteLog(1, "serverSocket Exception3: "+ex.toString());
                }
                catch (RejectedExecutionException r){
                    Log.WriteLog(1, "reject-serverSocket Exception: "+r.toString());
                    Log.WriteLog(1, "reject-serverSocket Stacktrace :"+Utils.getStackTrace(r));
                }catch (Exception ex) {
                    
                    Log.WriteLog(1, "4-serverSocket Exception: "+ex.toString());
                    Log.WriteLog(1, "4-serverSocket Stacktrace :"+Utils.getStackTrace(ex));
               
        
                } catch (Throwable ex2) {
                    
                    Log.WriteLog(1, "5-serverSocket Exception: "+ex2.toString());
                    Log.WriteLog(1, "5-serverSocket Stacktrace :"+Utils.getStackTrace(ex2));
                } 

                break;
            case 50:
                serverSocket.close();
                Runtime.getRuntime().addShutdownHook(new Thread()
                {
                @Override
                public void run()
                {
                    Log.WriteLog(1, "Shutdown thread pool!");
                    threadPool.shutdownNow();
                }
                });
                return false;

        }

        return true;
    }
    
    
}
