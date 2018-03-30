/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class ZB_ReservationProcess {

    DBAdminClass DBAdmin;
    ConfClass Conf;
    LogsClass Log;

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin, ConfClass LConf, LogsClass LLog) {
        DBAdmin = LDBAdmin;
        Conf = LConf;
        Log = LLog;
        return true;
    }

    public int CheckReservation_ZB_Stato() {

        Statement statement, statement1, statement2;

        ResultSet rs, rs1;
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

            statement = DBAdmin.DbAdminConn.createStatement();

            String QueryString = "SELECT * FROM blackbox where IDAzienda=42";
//            QueryString += " AND IDBlackBox=8152";

            rs = statement.executeQuery(QueryString);
            while (rs.next()) {
                CheckReservation_ZB_Stato(rs.getInt("IDBlackBox"));
            }
            rs.close();
            statement.close();

            String QueryString3 = "delete from reservation_zb_stato where IDBlackBox not in (SELECT IDBlackBox FROM blackbox where IDAzienda=42)";
//            System.out.println(QueryString3);
            statement = DBAdmin.DbAdminConn.createStatement();
            statement.execute(QueryString3);
            statement.close();

            DBAdmin.DbAdminConn.commit();

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }
//    public int CheckReservation_ZB_Stato() {
//        
//        Statement  statement,statement1,statement2;
//
//        ResultSet rs,rs1;
//        DBAdmin.CheckConnection();
//        try {
//            if (DBAdmin.DbAdminConn.isClosed())
//                return 0;
//        } catch (SQLException ex) {
//            return 0;
//        }
//        try {
//            DBAdmin.DbAdminConn.setAutoCommit(false);
//
//            statement = DBAdmin.DbAdminConn.createStatement();
//
//            String QueryString = "SELECT * FROM blackbox_debug.blackbox where IDAzienda=42";
////            QueryString += " AND IDBlackBox=8152";
//
//            rs = statement.executeQuery(QueryString);
//            while (rs.next()) {
//                int IDReservation=0;
//                int Stato =0;
//                String QueryString1 = "SELECT R.*,CURRENT_TIMESTAMP CurTime FROM blackbox_debug.reservation R where IDBlackBox="+rs.getInt("IDBlackBox")+
//                        " and ((StartTime>CURRENT_TIMESTAMP) or (StartTime<CURRENT_TIMESTAMP and EndTime>CURRENT_TIMESTAMP)) AND R.Enable<5 ";
//                
//                statement1 = DBAdmin.DbAdminConn.createStatement();
//                
//                rs1 = statement1.executeQuery(QueryString1);
//                if (rs1.next()) {
//                    IDReservation = rs1.getInt("IDReservation");
//                    if (rs1.getTimestamp("StartTime").before(rs1.getTimestamp("CurTime")) ) {
//                        Stato=2;
//                    } else {
//                        Stato=1;
//                    }
//                } else Stato =0;
//                
//                rs1.close();
//                statement1.close();
//       
//                String QueryString2 = "SELECT * FROM blackbox_debug.reservation_zb_stato where IDBlackBox="+rs.getInt("IDBlackBox");
//
//                statement1 = DBAdmin.DbAdminConn.createStatement();
//                rs1 = statement1.executeQuery(QueryString2);
//                if (rs1.next()) {
//                    if (rs1.getInt("Stato")!=Stato||rs1.getInt("IDReservation")!=IDReservation) { 
//                        String QueryString3 = "UPDATE blackbox_debug.reservation_zb_stato SET Stato="+Stato+ ", IDReservation="+IDReservation
//                                +" WHERE IDBlackBox="+rs.getInt("IDBlackBox");
//                        System.out.println(QueryString3);
//                        statement2 = DBAdmin.DbAdminConn.createStatement();
//                        statement2.execute(QueryString3);
//                        statement2.close();
//                    }
//                    if ((rs1.getInt("Stato")>=1) && Stato==0) {
//                        String QueryString3 = "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato, ReqID, Timeout, Time) VALUES ( ?, ?, ? ,?, ?,?)"; //+
//                        PreparedStatement statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString3);
//
//                        statement3.setInt(1, rs.getInt("IDBlackBox"));
//                        statement3.setString(2, "ZB-Code:5;00000000");
//                        statement3.setInt(3, 0);
//                        statement3.setString(4, "");
//                        statement3.setTimestamp(5, null);
//                        statement3.setTimestamp(6, (new java.sql.Timestamp(System.currentTimeMillis())));
//                        
//                        System.out.println(QueryString3);
//                        int res = statement3.executeUpdate();
//                        if (res==0) {
//                            System.out.println("Errore di inserimento del COMANDO nel DB");
//                        }
//                        statement3.close();
//                    }
//                } else {
//                    String QueryString3 = "INSERT INTO blackbox_debug.reservation_zb_stato (Stato, IDReservation, IDBlackBox) "
//                            + " VALUES ("+Stato+","+IDReservation+","+rs.getInt("IDBlackBox")+")";
//                    System.out.println(QueryString3);
//                    statement2 = DBAdmin.DbAdminConn.createStatement();
//                    statement2.execute(QueryString3);
//                    statement2.close();
//                }
//                rs1.close();
//                statement1.close();
//            }
//            rs.close();
//            statement.close();
//            
//            String QueryString3 ="delete from reservation_zb_stato where IDBlackBox not in (SELECT IDBlackBox FROM blackbox_debug.blackbox where IDAzienda=42)";
////            System.out.println(QueryString3);
//            statement = DBAdmin.DbAdminConn.createStatement();
//            statement.execute(QueryString3);
//            statement.close();
//            
//            DBAdmin.DbAdminConn.commit();
//            
//            return 1;
//        } catch (SQLException ex) {
//            Log.WriteEx(DBAdminClass.class.getName(), ex);
//            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            return 0;
//        } 
//    }

    public void CheckReservation_ZB_Stato(long IDBlackBox) throws SQLException {
        Statement statement, statement1, statement2;

        ResultSet rs, rs1;
        int IDReservation = 0;
        int Stato = 0;
        String QueryString1 = "SELECT R.*,CURRENT_TIMESTAMP CurTime FROM reservation R where IDBlackBox=" + IDBlackBox
                + " and ((StartTime>CURRENT_TIMESTAMP) or (StartTime<CURRENT_TIMESTAMP and EndTime>CURRENT_TIMESTAMP)) AND R.Enable<5 ";

        statement1 = DBAdmin.DbAdminConn.createStatement();

        rs1 = statement1.executeQuery(QueryString1);
        if (rs1.next()) {
            IDReservation = rs1.getInt("IDReservation");
            if (rs1.getTimestamp("StartTime").before(rs1.getTimestamp("CurTime"))) {
                Stato = 2;
            } else {
                Stato = 1;
            }
        } else {
            Stato = 0;
        }

        rs1.close();
        statement1.close();

        String QueryString2 = "SELECT * FROM reservation_zb_stato where IDBlackBox=" + IDBlackBox;

        statement1 = DBAdmin.DbAdminConn.createStatement();
        rs1 = statement1.executeQuery(QueryString2);
        if (rs1.next()) {
            if (rs1.getInt("Stato") != Stato || rs1.getInt("IDReservation") != IDReservation) {
                String QueryString3 = "UPDATE reservation_zb_stato SET Stato=" + Stato + ", IDReservation=" + IDReservation
                        + " WHERE IDBlackBox=" + IDBlackBox;
                System.out.println(QueryString3);
                statement2 = DBAdmin.DbAdminConn.createStatement();
                statement2.execute(QueryString3);
                statement2.close();
            }
            if ((rs1.getInt("Stato") >= 1) && Stato == 0) {
                String QueryString3 = "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato, ReqID, Timeout, Time) VALUES ( ?, ?, ? ,?, ?,?)"; //+
                PreparedStatement statement3 = DBAdmin.DbAdminConn.prepareStatement(QueryString3);

                statement3.setLong(1, IDBlackBox);
                statement3.setString(2, "ZB-Code:5;00000000");
                statement3.setInt(3, 0);
                statement3.setString(4, null);
                statement3.setTimestamp(5, null);
                statement3.setTimestamp(6, (new java.sql.Timestamp(System.currentTimeMillis())));

                System.out.println(QueryString3);
                int res = statement3.executeUpdate();
                if (res == 0) {
                    System.out.println("Errore di inserimento del COMANDO nel DB");
                }
                statement3.close();
            }
        } else {
            String QueryString3 = "INSERT INTO reservation_zb_stato (Stato, IDReservation, IDBlackBox) "
                    + " VALUES (" + Stato + "," + IDReservation + "," + IDBlackBox + ")";
            System.out.println(QueryString3);
            statement2 = DBAdmin.DbAdminConn.createStatement();
            statement2.execute(QueryString3);
            statement2.close();
        }
        rs1.close();
        statement1.close();

    }
}
