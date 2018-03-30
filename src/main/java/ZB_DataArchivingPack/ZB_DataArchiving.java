/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ZB_DataArchivingPack;

import ZB_DataAnalisysPack.*;
import com.viacom.DB.DBAdminClass;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import zb_udp_server.ConfClass;
import zb_udp_server.LogsClass;

/**
 *
 * @author Luca
 */
public class ZB_DataArchiving {

    DBAdminClass DBAdmin;
    ConfClass Conf;
    LogsClass Log;
    int RecordCount;
    ArrayList<String> message;

    public boolean SetEnvironmentClasses(DBAdminClass LDBAdmin, ConfClass LConf, LogsClass LLog) {
        DBAdmin = LDBAdmin;
        Conf = LConf;
        Log = LLog;
        return true;
    }

    public int ZBDataRunArchiving() {

        message = new ArrayList<String>();

        PreparedStatement statement1, statement2;
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
            Timestamp now = new Timestamp((new java.util.Date()).getTime());
            Timestamp LastRun = new Timestamp(0);
            Integer IDProcesses = null;

            DBAdmin.DbAdminConn.setAutoCommit(false);

            String QueryString = "SELECT * FROM processes WHERE ProcessName=?";

            statement1 = DBAdmin.DbAdminConn.prepareStatement(QueryString);

            statement1.setString(1, "DataArchiver");

            rs = statement1.executeQuery();
            if (rs.next()) {
                LastRun = rs.getTimestamp("LastUpdate");
                IDProcesses = rs.getInt("IDProcesses");
            }
            rs.close();
            statement1.close();

            if ((now.getTime() - LastRun.getTime()) > 24 * 60 * 60 * 1000) {      // il processo parte solo una volta al giorno 
                System.out.println("Start DataArchiver");
                RecordCount = 0;
                ArchiveRecords(now);

                ArchiveLocalization(now);

                ArchiveGuide(now);

                if (RecordCount < 500) {


                    if (IDProcesses == null) {
                        QueryString = "INSERT INTO Processes (LastUpdate,ProcessName) "
                                + " VALUES (?,\"DataArchiver\")";
                        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                        statement2.setTimestamp(1, new Timestamp(LastRun.getTime() + 24 * 60 * 60 * 1000));
                    } else {
                        QueryString = "UPDATE Processes SET LastUpdate=? "
                                + " where IDProcesses=?";
                        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
                        statement2.setTimestamp(1, new Timestamp(LastRun.getTime() + 24 * 60 * 60 * 1000));
                        statement2.setLong(2, IDProcesses);
                    }
                    statement2.execute();

                    statement2.close();

                }
                DBAdmin.DbAdminConn.commit();

                System.out.println("End DataArchiver");
            }

            return 1;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    int ArchiveRecords(Timestamp now) throws SQLException {
        Statement statement;
        PreparedStatement statement2;
        ResultSet rs2;

        String QueryString = "SELECT IDRec FROM zbrecords R "
                + "where R.Time<? and Stato=1 "
                + "order by IDRec "
                + "limit 500";
        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement2.setTimestamp(1, new Timestamp(now.getTime() - 6 * 24 * 60 * 60 * 1000));
        rs2 = statement2.executeQuery();
        statement = DBAdmin.DbAdminConn.createStatement();
        while (rs2.next() && RecordCount < 500) {
            statement.addBatch("insert into zbrecords_old SELECT * FROM zbrecords "
                    + "where IDRec=" + rs2.getLong("IDRec") + ";");
            statement.addBatch("DELETE FROM zbrecords "
                    + "where IDRec=" + rs2.getLong("IDRec") + ";");
//            System.out.println("IDRec="+rs2.getLong("IDRec"));
            RecordCount++;
        }
        System.out.println("Migrating " + RecordCount + " records from ZBRecords");
        statement.executeBatch();
        statement.close();
        rs2.close();
        statement2.close();

        return RecordCount;
    }

    int ArchiveLocalization(Timestamp now) throws SQLException {
        Statement statement;
        PreparedStatement statement2;
        ResultSet rs2;

        String QueryString = "SELECT idZBLocalization FROM zblocalization L "
                + "where L.BTimeStamp<? "
                + "order by idZBLocalization "
                + "limit 500";
        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement2.setTimestamp(1, new Timestamp(now.getTime() - 17 * 24 * 60 * 60 * 1000));
        rs2 = statement2.executeQuery();
        statement = DBAdmin.DbAdminConn.createStatement();
        String Str;
        while (rs2.next() && RecordCount < 500) {
            Str = "insert into zblocalization_old SELECT * FROM zblocalization "
                    + "where idZBLocalization=" + rs2.getLong("idZBLocalization") + ";";
            statement.addBatch(Str);
            System.out.println(Str);
            Str = "DELETE FROM zblocalization "
                    + "where idZBLocalization=" + rs2.getLong("idZBLocalization") + ";";
            statement.addBatch(Str);
            System.out.println(Str);
//            statement.addBatch("insert into zblocalization_old SELECT * FROM zblocalization "
//                    + "where idZBLocalization="+rs2.getLong("idZBLocalization")+";");
//            statement.addBatch("DELETE FROM zblocalization "
//                    + "where idZBLocalization="+rs2.getLong("idZBLocalization")+";");
            RecordCount++;
        }
        System.out.println("Migrating " + RecordCount + " records from zblocalization");
        statement.executeBatch();
        statement.close();
        rs2.close();
        statement2.close();

        return RecordCount;
    }

    int ArchiveGuide(Timestamp now) throws SQLException {
        Statement statement;
        PreparedStatement statement2;
        ResultSet rs2;

        String QueryString = "SELECT Z.idZBRecZ FROM zbrecz Z "
                + "where Z.BTimeStamp<? and ReceiveComplete=1 "
                + "order by idZBRecZ "
                + "limit 20";
        statement2 = DBAdmin.DbAdminConn.prepareStatement(QueryString);
        statement2.setTimestamp(1, new Timestamp(now.getTime() - 15 * 24 * 60 * 60 * 1000));
        rs2 = statement2.executeQuery();
        statement = DBAdmin.DbAdminConn.createStatement();
        String Str;
        while (rs2.next() && RecordCount < 500) {
            Str = "insert into zbrecz_old SELECT * FROM zbrecz "
                    + "where idZBRecZ=" + rs2.getLong("idZBRecZ") + ";";
            statement.addBatch(Str);
            System.out.println(Str);
            Str = "insert into zbrecxy_old SELECT * FROM zbrecxy "
                    + "where idZBRecZ=" + rs2.getLong("idZBRecZ") + ";";
            statement.addBatch(Str);
            System.out.println(Str);
            Str = "DELETE FROM zbrecz "
                    + "where idZBRecZ=" + rs2.getLong("idZBRecZ") + ";";
            statement.addBatch(Str);
            System.out.println(Str);
            Str = "DELETE FROM zbrecxy "
                    + "where idZBRecZ=" + rs2.getLong("idZBRecZ") + ";";
            statement.addBatch(Str);
            System.out.println(Str);
            RecordCount += 25;
        }
        System.out.println("Migrating " + RecordCount + " records from zbrecz and zbrecxy");
        statement.executeBatch();
        statement.close();
        rs2.close();
        statement2.close();

        return RecordCount;
    }
}
