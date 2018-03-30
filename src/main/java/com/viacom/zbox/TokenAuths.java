/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viacom.zbox;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.Token;
import com.viacom.zbox.TokenAuth;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class TokenAuths {

    Connection DB;
    public List<TokenAuth> Auth = Collections.synchronizedList(new ArrayList<TokenAuth>());

    public TokenAuths() {
//        TokenHWID_N=-1;
//        Start=0;
//        End=0;
//        Status=-1;
    }

    public TokenAuths(Connection LDB) {
        super();
        DB = LDB;
    }

    public void SetConnection(Connection LDB) {
        DB = LDB;
    }
    /* Legge la lista del reservation per la ZB indicata da IDBlackBox ed il Token indicato da IDToken
     * @return 
     */

    public int ReadAuth(long IDBlackBox, long IDToken, long TokenHWID_N, boolean Resend, Token Tk) {
        int ret = 0;
        PreparedStatement statement = null;
        ResultSet rs = null;

        SimpleDateFormat FullDateFormat = new SimpleDateFormat("[dd/MM/yyyy HH:mm:ss.SSSS] ");
        String FullDate = FullDateFormat.format(new Date());

        try {
            if (Tk.IDAzienda == 44) {
                ret = ReadAuthVeritas(IDBlackBox, Tk, Resend);
            } else if (Tk.IDAzienda == 46) {
                ret = ReadAuthUnicredit(IDBlackBox, Tk, Resend);
            } else {
                try {
                    int reservation_zb_stato = 1;
                    // verfica se la vettura è libera o prenotata
                    String QueryString = "SELECT R.* FROM reservation_zb_stato R "
                            + " WHERE R.IDBlackBox=" + IDBlackBox;
                    System.out.println("TokenHWID_N " + QueryString);

                    statement = DB.prepareStatement(QueryString);
                    rs = statement.executeQuery();
                    if (rs.next()) {
                        if (rs.getInt("Stato") >= 0) {
                            reservation_zb_stato = rs.getInt("Stato");
                        }
                    }
                    statement.close();
                    rs.close();
                    
//                    System.out.println("TokenHWID_N reservation_zb_stato" + reservation_zb_stato);
//
                    if (reservation_zb_stato > 0) {   // veicolo prenotato o occupato
                        // esegue la query al DB
                        QueryString = "SELECT * from reservation R LEFT JOIN Token T ON R.IDToken=T.IDToken "
                                + " WHERE R.IDBlackBox=" + IDBlackBox + " AND (Enable=0 or Enable=1) ";
                        if (!Resend) {
                            QueryString += "AND (R.stato=0 OR R.stato=1)";
                        }
                        if (IDToken >= 0) {
                            QueryString += " and R.IDToken=" + IDToken;
                        }

                        QueryString += " ORDER BY R.StartTime LIMIT 10";

                        System.out.println("TokenHWID_N " + QueryString);

                        statement = DB.prepareStatement(QueryString);
                        rs = statement.executeQuery();
                        while (rs.next()) {
                            TokenAuth A = new TokenAuth();
                            A.IDReservation = rs.getLong("IDReservation");
                            A.TokenHWID_N = rs.getLong("TokenHWID_N");
                            A.Status = rs.getInt("Enable");
                            A.Start = rs.getTimestamp("StartTime").getTime() / 1000;
                            A.End = rs.getTimestamp("EndTime").getTime() / 1000;
                            System.out.println(FullDate + "Token IDReservation:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N
                                    + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End);
                            Auth.add(A);
                        }
                        if (statement != null) {
                            statement.close();
                        }

                        for (int i = 0; i < Auth.size(); i++) {
                            QueryString = "UPDATE reservation SET Stato=1,SendTime=? WHERE IDReservation=" + Auth.get(i).IDReservation;
                            statement = DB.prepareStatement(QueryString);
                            Timestamp Now = new Timestamp((new java.util.Date()).getTime());
                            statement.setTimestamp(1, Now);
                            statement.execute();
                        }
                        if ((IDToken >= 0) && Auth.isEmpty()) {
                            // in caso di mancata presenza di un record di abilitazione invio una risposta di disabilitazione per 5 minuti
                            java.sql.Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
                            TokenAuth A = new TokenAuth();
                            A.IDReservation = -1;
                            A.TokenHWID_N = TokenHWID_N;
                            A.Status = 0;
//                        A.Start=(now.getTime()-(5*60*1000))/1000;
//                        A.End=(now.getTime()+(5*60*1000))/1000;
                            A.Start = (now.getTime() - (30 * 1000)) / 1000;
                            A.End = (now.getTime() + (30 * 1000)) / 1000;
                            System.out.println(FullDate + "Token IDReservation:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N
                                    + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End
                                    + " Now:" + now.toString() + " Start:" + (new Timestamp(A.Start * 1000)).toString()
                                    + " End:" + (new Timestamp(A.End * 1000)).toString());
                            Auth.add(A);

                        }
                        ret = Auth.size();
                    } else {
                        QueryString = "SELECT R.*,CURRENT_TIMESTAMP CurTime FROM reservation_passepartout R where R.IDToken is not null "+
                                " AND R.Active>=1 ";
                        if (IDToken >= 0) {
                            QueryString += " and R.IDToken=" + IDToken;
                        }
                        
//                        System.out.println(FullDate + " TokenHWID_N:" + QueryString);
//
                        statement = DB.prepareStatement(QueryString);
                        rs = statement.executeQuery();
                        while (rs.next()) {
                            TokenAuth A = new TokenAuth();
                            A.IDReservation = rs.getLong("idreservation_passepartout");
                            A.TokenHWID_N = TokenHWID_N;
                            A.Status = 1;
                            A.Start = (rs.getTimestamp("CurTime").getTime() - (60 * 1000)) / 1000;
                            A.End = (rs.getTimestamp("CurTime").getTime() + (30 * 1000)) / 1000;
                            System.out.println(FullDate + "Token reservation_passepartout:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N
                                    + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End);
                            Auth.add(A);
                        }
                        if (statement != null) {
                            statement.close();
                        }

                        if ((IDToken >= 0) && Auth.isEmpty()) {
                            // in caso di mancata presenza di un record di abilitazione invio una risposta di disabilitazione per 5 minuti
                            java.sql.Timestamp now = new java.sql.Timestamp((new java.util.Date()).getTime());
                            TokenAuth A = new TokenAuth();
                            A.IDReservation = -1;
                            A.TokenHWID_N = TokenHWID_N;
                            A.Status = 0;
//                        A.Start=(now.getTime()-(5*60*1000))/1000;
//                        A.End=(now.getTime()+(5*60*1000))/1000;
                            A.Start = (now.getTime() - (30 * 1000)) / 1000;
                            A.End = (now.getTime() + (30 * 1000)) / 1000;
                            System.out.println(FullDate + "Token IDReservation:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N
                                    + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End
                                    + " Now:" + now.toString() + " Start:" + (new Timestamp(A.Start * 1000)).toString()
                                    + " End:" + (new Timestamp(A.End * 1000)).toString());
                            Auth.add(A);

                        }
                        ret = Auth.size();
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                    if (statement != null) {
                        statement.close();
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ret;
    }

    public int ReadAuthVeritas(long IDBlackBox, Token Tk, boolean Resend) {
        SimpleDateFormat FullDateFormat = new SimpleDateFormat("[dd/MM/yyyy HH:mm:ss.SSSS] ");
        String FullDate = FullDateFormat.format(new Date());
        int ret = 0;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            try {
                String QueryString = "SELECT * from reservation R LEFT JOIN Token T ON R.IDToken=T.IDToken  WHERE R.IDBlackBox=" + IDBlackBox + " AND (Enable=0 or Enable=1) and CURRENT_TIMESTAMP<=R.EndTime ";

//                if (!Resend) {
//                    QueryString = QueryString + "AND (R.stato=0 OR R.stato=1)";
//                }
                if (Tk.IDToken >= 0) {
                    QueryString = QueryString + " and R.IDToken=" + Tk.IDToken;
                }
                QueryString = QueryString + " ORDER BY R.StartTime LIMIT 10";

                System.out.println("TokenHWID_N Veritas " + QueryString);

                statement = this.DB.prepareStatement(QueryString);
                rs = statement.executeQuery();
                while (rs.next()) {
                    TokenAuth A = new TokenAuth();
                    A.IDReservation = rs.getLong("IDReservation");
                    A.TokenHWID_N = rs.getLong("TokenHWID_N");
                    A.Status = rs.getInt("Enable");
                    A.Start = (rs.getTimestamp("StartTime").getTime() / 1000);
                    A.End = (rs.getTimestamp("EndTime").getTime() / 1000);
                    System.out.println(FullDate + "Token IDReservation:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End);

                    this.Auth.add(A);
                }
                if (statement != null) {
                    statement.close();
                }
                if (this.Auth.size() > 0) {
                    for (int i = 0; i < this.Auth.size(); i++) {
                        QueryString = "UPDATE reservation SET Stato=1,SendTime=? WHERE IDReservation=" + ((TokenAuth) this.Auth.get(i)).IDReservation;
                        statement = this.DB.prepareStatement(QueryString);
                        Timestamp Now = new Timestamp(new Date().getTime());
                        statement.setTimestamp(1, Now);
                        statement.execute();
                    }
                } else if (Tk.isPasspartoutActive) {
                    Timestamp now = new Timestamp(new Date().getTime());
                    TokenAuth A = new TokenAuth();
                    A.IDReservation = Tk.idreservation_passepartout;
                    A.TokenHWID_N = Tk.TokenHWID_N;
                    A.Status = 1;
                    A.Start = ((now.getTime() - 30000) / 1000);
                    A.End = ((now.getTime() + 30000) / 1000);
                    System.out.println("Token reservation_passepartout:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End);

                    this.Auth.add(A);
                }


                if ((Tk.IDToken >= 0) && (this.Auth.isEmpty())) {
                    Timestamp now = new Timestamp(new Date().getTime());
                    TokenAuth A = new TokenAuth();
                    A.IDReservation = -1L;
                    A.TokenHWID_N = Tk.TokenHWID_N;
                    A.Status = 0;
                    A.Start = ((now.getTime() - 30000) / 1000);
                    A.End = ((now.getTime() + 30000) / 1000);
                    System.out.println("Token IDReservation:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End + " Now:" + now.toString() + " Start:" + new Timestamp(A.Start * 1000L).toString() + " End:" + new Timestamp(A.End * 1000L).toString());



                    this.Auth.add(A);
                }
                ret = this.Auth.size();
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ret;
    }

    public int ReadAuthUnicredit(long IDBlackBox, Token Tk, boolean Resend) {
        SimpleDateFormat FullDateFormat = new SimpleDateFormat("[dd/MM/yyyy HH:mm:ss.SSSS] ");
        String FullDate = FullDateFormat.format(new Date());
        int ret = 0;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            try {
                String QueryString = "SELECT * from reservation R LEFT JOIN Token T ON R.IDToken=T.IDToken  WHERE R.IDBlackBox=" + IDBlackBox + " AND (Enable=0 or Enable=1) and CURRENT_TIMESTAMP<=R.EndTime ";

//                if (!Resend) {
//                    QueryString = QueryString + "AND (R.stato=0 OR R.stato=1 OR R.stato=10 )";
//                }
                if (Tk.IDToken >= 0) {
                    QueryString = QueryString + " and R.IDToken=" + Tk.IDToken;
                }
                QueryString = QueryString + " ORDER BY R.StartTime LIMIT 10";

                System.out.println("TokenHWID_N Unicredit " + QueryString);

                statement = this.DB.prepareStatement(QueryString);
                rs = statement.executeQuery();
                while (rs.next()) {
                    TokenAuth A = new TokenAuth();
                    A.IDReservation = rs.getLong("IDReservation");
                    A.TokenHWID_N = rs.getLong("TokenHWID_N");
                    A.Status = rs.getInt("Enable");
                    A.Start = (rs.getTimestamp("StartTime").getTime() / 1000);
                    A.End = (rs.getTimestamp("EndTime").getTime() / 1000);
                    System.out.println(FullDate + "Token IDReservation:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End);

                    this.Auth.add(A);
                }
                if (statement != null) {
                    statement.close();
                }
                if (this.Auth.size() > 0) {
                    for (int i = 0; i < this.Auth.size(); i++) {
                        if (((TokenAuth) this.Auth.get(i)).Status != 10) {
                            QueryString = "UPDATE reservation SET Stato=1,SendTime=? WHERE IDReservation=" + ((TokenAuth) this.Auth.get(i)).IDReservation;
                            statement = this.DB.prepareStatement(QueryString);
                            Timestamp Now = new Timestamp(new Date().getTime());
                            statement.setTimestamp(1, Now);
                            statement.execute();
                        }
                    }
                } else if (Tk.isPasspartoutActive) {
                    Timestamp now = new Timestamp(new Date().getTime());
                    TokenAuth A = new TokenAuth();
                    A.IDReservation = Tk.idreservation_passepartout;
                    A.TokenHWID_N = Tk.TokenHWID_N;
                    A.Status = 1;
                    A.Start = ((now.getTime() - 30000) / 1000);

                    A.End = (new Timestamp(now.getYear(), now.getMonth(), now.getDate(), 23, 59, 59, 0).getTime() / 1000L);

                    System.out.println(FullDate + "Token reservation_passepartout:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End);

                    QueryString = "INSERT INTO reservation (IDBlackBox, IDToken, StartTime, EndTime, ReqID, SendTime, Enable, Stato) VALUES (?, ?, ?, ?, ?, ?, 1, 10)";
                    System.out.println("TokenHWID_N Unicredit " + QueryString);
                    statement = this.DB.prepareStatement(QueryString);
                    Timestamp Now = new Timestamp(new Date().getTime());
                    statement.setLong(1, IDBlackBox);
                    statement.setLong(2, Tk.IDToken);
                    statement.setTimestamp(3, new Timestamp(A.Start * 1000));
                    statement.setTimestamp(4, new Timestamp(A.End * 1000));
                    statement.setString(5, "-1");
                    statement.setTimestamp(6, Now);

                    statement.execute();
                    this.Auth.add(A);
                }


                if ((Tk.IDToken >= 0L) && (this.Auth.isEmpty())) {
                    Timestamp now = new Timestamp(new Date().getTime());
                    TokenAuth A = new TokenAuth();
                    A.IDReservation = -1L;
                    A.TokenHWID_N = Tk.TokenHWID_N;
                    A.Status = 0;
                    A.Start = ((now.getTime() - 30000) / 1000);
                    A.End = ((now.getTime() + 30000) / 1000);
                    System.out.println(FullDate + "Token IDReservation:" + A.IDReservation + " TokenHWID_N:" + A.TokenHWID_N + " Status:" + A.Status + " Start:" + A.Start + " End:" + A.End + " Now:" + now.toString() + " Start:" + new Timestamp(A.Start * 1000L).toString() + " End:" + new Timestamp(A.End * 1000L).toString());



                    this.Auth.add(A);
                }
                ret = this.Auth.size();
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ret;
    }


    /* Legge la lista del reservation per la ZB indicata da IDBlackBox ed il Token indicato da IDToken
     * @return 
     */
    public int SetReceiveAck() {
        int ret = 0;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            try {
                String QueryString = "";
                for (int i = 0; i < Auth.size(); i++) {
                    QueryString = "UPDATE reservation SET Stato=2 where IDReservation=" + Auth.get(i).IDReservation;
                    statement = DB.prepareStatement(QueryString);
                    statement.execute();
                }
                ret = Auth.size();
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ret;
    }
}
