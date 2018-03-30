/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.mysql.jdbc.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import snaq.db.ConnectionPool;

//import java.util.logging.Level;
//import java.util.logging.Logger;
/**
 *
 * @author Luca
 */
public class DBConnector {

    public String Version = "ZBox UDP Server v0.1";
    public String userName = "CABlackBox";
    public String password = "3rYXMcEENRPLHuSK";
    public String url = "jdbc:mysql://192.168.1.237/";
    public String AdminDBName = "blackbox_debug";
    public String LogPath = "C:\\DT\\";
    public int LogLevel = 1;
    public boolean debug = true;
    public String ComPort = "COM2";
    public int ParseRecord = 0;
    public int SWUpdateProcess = 0;
    public int DataAnalizerProcess = 0;
    public int CertidriveCheckData = 0;
    public int CertidriveCheckRecords = 0;
    public int CheckPushNotify = 0;
    public int DataArchiver = 0;
    public int ListenPort = 8802;
    public Timestamp LogicTimeStamp;
    public String CertidriveUserName = "viacom_db_user";
    public String CertidrivePassword = "asd43mGx3";
    public String CertidriveUrl = "jdbc:mysql://192.168.1.3/";
    public String CertidriveAdminDBName = "cddb_viacom_adm";
    ConfClass Conf;
    LogsClass Log;
    public ConnectionPool PoolBB;
//    public ConnectionPool PoolBBInstall;
//    public ConnectionPool PoolBBInstallFase2;
    public ConnectionPool PoolTaxiDB;
    private static DBConnector instance = null;

    public static DBConnector getInstance() {
        if (instance == null) {
            instance = new DBConnector();
        }
        return instance;
    }

    public DBConnector() {
        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
    }

    /**
     * La funzione legge i parametri di configurazione dal Config file passato
     * come parametro
     *
     * @param FileName
     */
    public boolean Init() {
        String userName = "viacom_db_user";
        String password = "viacom_db_user";
        String url = "jdbc:mysql://192.168.1.237/blackbox_debug?autoReconnect=true";
//        String driverString = "com.mysql.jdbc.Driver";
        String driverString = "com.mysql.jdbc.Driver";
        userName = Conf.userName;
        password = Conf.password;
        url = Conf.url + Conf.AdminDBName;

//        String userName2 = "DBMysql";
//        String password2 = "DB198Mysql>";
//        String url2 = "jdbc:mysql://192.168.1.246:3306/simplerp?autoReconnect=true";
//        
//        String userName3 = "DBMysql";
//        String password3 = "DB198Mysql>";
//        String url3 = "jdbc:mysql://192.168.1.246:3306/simplerpv2?autoReconnect=true";

//        String UserNameTaxiDB = "DBMysql";
//        String PasswordTaxiDB = "DB198Mysql>";
//        String UrlTaxiDB = "jdbc:mysql://192.168.1.246:3306/taxi?autoReconnect=true";
        String UserNameTaxiDB = Conf.UserNameTaxiDB;
        String PasswordTaxiDB = Conf.PasswordTaxiDB;
        String UrlTaxiDB = Conf.UrlTaxiDB + Conf.TaxiDBName + "?autoReconnect=true";


        Class c;
        try {
            c = Class.forName(driverString);
            Driver driver;

            driver = (Driver) c.newInstance();
            DriverManager.registerDriver(driver);

            // Note, timeout is specified in milliseconds.
            PoolBB = new ConnectionPool("PoolBB", 100, 600, 600, 600, url, userName, password);
            //PROVA
            //PoolBB = new ConnectionPool("PoolBB", 500, 1000, 1000, 5000, url, userName, password);
           
            
//            PoolBB = new ConnectionPool("PoolBB", 5, 10, 30, 1000, url, userName, password);
//            PoolBBInstall = new ConnectionPool("PoolBBInstall",5, 10, 30, 1000, url2, userName2, password2);
//            PoolBBInstallFase2 = new ConnectionPool("PoolBBInstallFase2",5, 10, 30, 1000, url3, userName3, password3);
            if (Conf.TaxiDataCheck > 0) {
                PoolTaxiDB = new ConnectionPool("PoolBBInstallFase2", 5, 10, 20, 1000, UrlTaxiDB, UserNameTaxiDB, PasswordTaxiDB);
            }
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        } catch (InstantiationException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        } catch (IllegalAccessException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        } catch (SQLException ex) {
            Logger.getLogger(DBConnector.class.getName()).log(Level.SEVERE, null, ex);
            return false;
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
}
