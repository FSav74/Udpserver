/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.TaxiPack;
import com.viacom.zbox.zbox;
import com.viacom.zbox.zboxtaxi;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import org.bouncycastle.util.encoders.Hex;

/**
 *
 * @author Luca
 */
public class TCPTaxiConnect extends Thread {

    private ConfClass Conf;
    private LogsClass Log;
    public String TaxiServerIP = "82.187.10.113";
    public int TaxiPort = 1200;
    ServerSocket SSListener = null;
    DBConnector DBConn = DBConnector.getInstance();
    Connection DBBB;
    Connection DBTaxi;
    ArrayList<TaxiTCPConn> TCPConnectionList;

    class Taxi_packet {

        byte SessionID[] = new byte[4];
        byte PackN;
        byte Type;
        byte Size;
        byte Crypt;
        byte Spare;
        byte Fill;
        byte Payload[];
    };

    private class TaxiTCPConn {

        zboxtaxi ZB;
        BufferedReader in = null;
//        DataOutputStream out = null;
        BufferedOutputStream out = null;
        Socket ClientSocket;
        TaxiTCPReceiverTask ReceiveTask;
        Taxi_packet CurrentPack = null;

        public void StartReceiver() {
            ReceiveTask = new TaxiTCPReceiverTask(ClientSocket);
            ReceiveTask.start();
        }

        public class TaxiTCPReceiverTask extends Thread {

            Socket ClientSocket;
            boolean running;

            private TaxiTCPReceiverTask(Socket connection) {
                //throw new UnsupportedOperationException("Not yet implemented");
                ClientSocket = connection;
                running = false;
            }

            @Override
            public void run() {
                running = true;

                byte inputBuffer[] = new byte[4048];

                try {
                    DataInputStream inputS = new DataInputStream(ClientSocket.getInputStream());

                    int read;
                    while (((read = inputS.read(inputBuffer)) != -1) && running) {
                        System.out.println("byte read " + read);
                        System.out.println(" Data: " + (new String(Hex.encode(inputBuffer))));

                        Taxi_packet Pk = new Taxi_packet();
                        if (ReadTaxiPack(Pk, (byte) 0, inputBuffer) > 0) {
                            CurrentPack = Pk;
                        }

                        if (ClientSocket.isClosed()) {
                            System.out.println("ClientSocket closed");
                            running = false;
                        }

                    }
                } catch (IOException ex) {
                    Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
                    running = false;
                }
                System.out.println("Session closed");

//                while (running) {
//                    try {
//                        if (in.ready()) {
//                            
//                        }
//                        if  (ClientSocket.isClosed()) running = false ;
//                    }
////                    catch (InterruptedIOException iioe) {
////                        running = false ;
////                    }
//                    catch (IOException ioe) {
//                        running = false ;
//                    }
//
//                    // nap for a bit
//                    try {
//                        Thread.sleep(50);
//                    }
//                    catch (InterruptedException ie) {
//                    }
//                }


            }

            /**
             * Legge un packet dal buffer di trasmissione GPRS e ne verifica la
             * validità. Se il pacchetto è valido ritorna 1 e popola la
             * struttura Pk con i dati del pacchetto. Altrimenti ritorna 0
             *
             * @return: 0 pacchetto non valido 1 pacchetto valido e copiato in
             * Pk
             */
            int ReadTaxiPack(Taxi_packet Pk, byte ServerNum, byte data[]) {
                int SessionID = 0;
                try {
                    int i = 0;
                    // verifica SOM
                    //            System.out.println("Verifica SOM");
                    if (data[i++] != (byte) 0xa5) {
                        return 0;
                    }
                    if (data[i++] != (byte) 0x5a) {
                        return 0;
                    }

                    //            System.out.println("Identificato SOM");
                    System.arraycopy(data, i, Pk.SessionID, 0, 4);
                    i += 4;
                    Pk.PackN = data[i++];
                    Pk.Type = data[i++];
                    Pk.Size = data[i++];
                    Pk.Crypt = (byte) ((data[i] >> 7) & 0x1);
                    Pk.Fill = (byte) (data[i++] & 0xF);
                    Pk.Payload = new byte[Pk.Size * 16];
                    System.arraycopy(data, i, Pk.Payload, 0, Pk.Size * 16);
                } catch (ArrayIndexOutOfBoundsException Ex) {
                    Log.WriteLog(2, "ID= " + SessionID + " ReadGPRSPack: errore di interpretazione del pacchetto : " + (new String(Hex.encode(data))));
                }

                int nByte = Pk.Size * 16 + 10;
                if (nByte > data.length + 10) {
                    Log.WriteLog(2, "ID= " + SessionID + " Packet size non valido Pk.Size=" + (nByte));
                }

                //        Log.WriteLog(3,"Verifica CRC per byte="+(nByte));
                // verifica del crc
                MessageDigest cript;

                try {
                    cript = MessageDigest.getInstance("SHA-1");
                    cript.reset();
                    cript.update(data, 0, nByte);
                } catch (NoSuchAlgorithmException ex) {
                    Log.WriteEx(ZB_UDP_Comunication.class.getName(), ex);
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                    return 0;
                }

                byte[] CRC = cript.digest();
                String hexString = (new String(Hex.encode(data))).substring(0, (nByte + 3) * 2);
                //        Log.WriteLog(3,"ID= "+SessionID+" ReadGPRSPack Data:"+hexString);

                //        hexString = new String(Hex.encode(CRC));
                //        System.out.println("CRC :"+hexString);

                if (!(CRC[0] == data[nByte + 0]
                        && CRC[1] == data[nByte + 1]
                        && CRC[2] == data[nByte + 2])) {
                    Log.WriteLog(1, "ID= " + SessionID + " CRC non verificato");
                    return 0;
                }

                // se il pacchetto è cifrato provvedo a decifrarlo
                if (Pk.Crypt > 0) {
                    //                zbox ZB=DBAdmin.GetZBox(DB, Pk.SessionID);
                    //                Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());   
                    //    //            byte[] keyBytes = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
                    //    //            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17 };
                    //
                    //                SecretKeySpec key = new SecretKeySpec(ZB.AESKeyOut, "AES");
                    //                try {
                    //                    Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
                    //                    try {
                    //                        cipher.init(Cipher.DECRYPT_MODE, key);
                    //                        Pk.Payload=cipher.doFinal(Pk.Payload);
                    //                    } catch (IllegalBlockSizeException ex) {
                    //                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                    //                    } catch (BadPaddingException ex) {
                    //                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                    //                    } catch (InvalidKeyException ex) {
                    //                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                    //                    }
                    //                } catch (NoSuchAlgorithmException ex) {
                    //                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                    //                } catch (NoSuchProviderException ex) {
                    //                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                    //                } catch (NoSuchPaddingException ex) {
                    //                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                    //                }
                }
                if (Pk.SessionID[0] == 0 && Pk.SessionID[1] == 0 && Pk.SessionID[2] == 0 && Pk.SessionID[3] == 0) { // Seriale di ZB non valido
                    return 0;
                }
                return 1;
            }
        }
    }

    public void TestCode() {
        Statement statement;
        ResultSet rs;


        System.out.println("TestCode");
//        System.out.println("byte read "+read);
//        System.out.println(" Data: "+(new String(Hex.encode(inputBuffer))));
        try {
            DBTaxi = DBConn.PoolTaxiDB.getConnection();
            DBTaxi.setAutoCommit(true);
            // esegue la query al DB
//            statement = DBTaxi.createStatement();
//        
//            String QueryString = "SELECT IDFromZB,IDBlackBox,Data,ReceiveTimeStamp,TransmitTimeStamp,stato FROM taxi.fromzb "
//                    + "where IDBlackBox=8287 and IDFromZB>=450";
//
//            rs = statement.executeQuery(QueryString);
//
//            while (rs.next()) {
//                byte [] d=rs.getBytes("Data");
//                String Da=toHexString(d);
//                int IDFromZB=rs.getInt("IDFromZB");
//                System.out.println(" IDFromZB="+IDFromZB +" Data: "+Da);
//            }
//            rs.close();
//            statement.close();

            String D2 = "7100393931393901";
            byte[] D3 = toByteArray(D2);
            java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());

            String QueryString1 = "INSERT INTO fromzb (IDBlackBox,Data,ReceiveTimeStamp,stato) VALUES (?, ?, ?, 0)";
            PreparedStatement statement1;
            statement1 = DBTaxi.prepareStatement(QueryString1, Statement.RETURN_GENERATED_KEYS);
//            Log.WriteLog(3,"ID= "+SessionID+" ZB :"+ new String(Hex.encode(Pk.SessionID))+" insert data:"+new String(Hex.encode(Pk.Payload)));
            statement1.setInt(1, 8287);  //IDBlackBox
            statement1.setBytes(2, D3);
            statement1.setTimestamp(3, now1);

            statement1.execute();



        } catch (SQLException ex) {
            Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static String toHexString(byte[] array) {
        return DatatypeConverter.printHexBinary(array);
    }

    public static byte[] toByteArray(String s) {
        return DatatypeConverter.parseHexBinary(s);
    }
//    private class ThreadedClass extends Thread {
//        Socket ClientSocket;
//        private ThreadedClass(Socket connection) {
//            //throw new UnsupportedOperationException("Not yet implemented");
//            ClientSocket= connection;
//        }
//        
//        @Override
//        @SuppressWarnings("SleepWhileInLoop")
//        public void run() {
//            PrintWriter out = null;
//            try {
//                byte inputBuffer[]= new byte[4048];
//                out = new PrintWriter( ClientSocket.getOutputStream(), true);
//                BufferedReader in = new BufferedReader( new InputStreamReader(ClientSocket.getInputStream()));
//                
//                DataInputStream inputS = new DataInputStream(ClientSocket.getInputStream());
//                
//                String inputLine, outputLine;
//                
//                int read;
//                while((read = inputS.read(inputBuffer)) != -1) {
//                    System.out.println("byte read "+read);
//                    System.out.println(" Data: "+(new String(Hex.encode(inputBuffer))));
//                    
//                    Taxi_packet Pk = new Taxi_packet();
//                    if (ReadTaxiPack(Pk,(byte)0,inputBuffer)>0){
//                        
//                    }
//                    
//                }
//                System.out.println("Session closed");
//                // initiate conversation with client
//                
////                out.println("HELO\n\r");
////                while (!in.ready()) {
////                    in.read(inputBuffer);
////                    try {
////                        Thread.sleep(50);
////                    } catch (InterruptedException ex) {
////                        Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
////                    }
////                }
//                try {
//                        Thread.sleep(500);
//                    } catch (InterruptedException ex) {
//                        Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//                    }
///*                while ((inputLine = in.readLine()) != null) {
//                    System.out.println("Input : "+inputLine);
//                    outputLine = processInput(inputLine);
//                    System.out.println("Output: "+outputLine);
//                    out.println(outputLine);
//                    if (outputLine.equals("Bye."))
//                        break;
//                }*/
//               
//            } catch (IOException ex) {
//                Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//            } finally {
//                out.close();
//            }
//
//        }
//        
//        /**
//        * Legge un packet dal buffer di trasmissione GPRS e ne verifica la validità. Se il pacchetto è
//        * valido ritorna 1 e popola la struttura Pk con i dati del pacchetto.
//        * Altrimenti ritorna 0
//        * @return:  0 pacchetto non valido
//        * 		1 pacchetto valido e copiato in Pk
//        */
//        int ReadTaxiPack (Taxi_packet Pk, byte ServerNum, byte data[]) {
//            int SessionID=0;
//            try {
//                int i=0;
//                // verifica SOM
//    //            System.out.println("Verifica SOM");
//                if (data[i++]!=(byte)0xa5) return 0;
//                if (data[i++]!=(byte)0x5a) return 0;
//
//    //            System.out.println("Identificato SOM");
//                System.arraycopy(data, i, Pk.SessionID, 0, 4);
//                i+=4;
//                Pk.PackN=data[i++];
//                Pk.Type=data[i++];
//                Pk.Size=data[i++];
//                Pk.Crypt=(byte)((data[i]>>7)&0x1);
//                Pk.Fill=(byte)(data[i++]&0xF);
//                Pk.Payload=new byte[Pk.Size*16];
//                System.arraycopy(data, i, Pk.Payload, 0, Pk.Size*16);
//            } catch (ArrayIndexOutOfBoundsException Ex) {
//                Log.WriteLog(2,"ID= "+SessionID+" ReadGPRSPack: errore di interpretazione del pacchetto : "+(new String(Hex.encode(data))));
//            }
//
//            int nByte=Pk.Size*16+10;
//            if (nByte>data.length+10) {
//                Log.WriteLog(2,"ID= "+SessionID+" Packet size non valido Pk.Size="+(nByte));
//            }
//
//    //        Log.WriteLog(3,"Verifica CRC per byte="+(nByte));
//            // verifica del crc
//            MessageDigest cript;
//
//            try {
//                cript = MessageDigest.getInstance("SHA-1");
//                cript.reset();
//                cript.update(data,0,nByte);
//            } catch (NoSuchAlgorithmException ex) {
//                Log.WriteEx(ZB_UDP_Comunication.class.getName(), ex);
//                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
//                return 0;
//            }
//
//            byte [] CRC=cript.digest();
//            String hexString = (new String(Hex.encode(data))).substring(0,(nByte+3)*2);
//    //        Log.WriteLog(3,"ID= "+SessionID+" ReadGPRSPack Data:"+hexString);
//
//    //        hexString = new String(Hex.encode(CRC));
//    //        System.out.println("CRC :"+hexString);
//
//            if (!(CRC[0]==data[nByte+0]&&
//                CRC[1]==data[nByte+1]&&
//                CRC[2]==data[nByte+2])) {
//                    Log.WriteLog(1,"ID= "+SessionID+" CRC non verificato");
//                    return 0;
//            }
//
//            // se il pacchetto è cifrato provvedo a decifrarlo
//            if (Pk.Crypt>0) {
////                zbox ZB=DBAdmin.GetZBox(DB, Pk.SessionID);
////                Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());   
////    //            byte[] keyBytes = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
////    //            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17 };
////
////                SecretKeySpec key = new SecretKeySpec(ZB.AESKeyOut, "AES");
////                try {
////                    Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
////                    try {
////                        cipher.init(Cipher.DECRYPT_MODE, key);
////                        Pk.Payload=cipher.doFinal(Pk.Payload);
////                    } catch (IllegalBlockSizeException ex) {
////                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
////                    } catch (BadPaddingException ex) {
////                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
////                    } catch (InvalidKeyException ex) {
////                        Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
////                    }
////                } catch (NoSuchAlgorithmException ex) {
////                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
////                } catch (NoSuchProviderException ex) {
////                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
////                } catch (NoSuchPaddingException ex) {
////                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
////                }
//            }
//            if (Pk.SessionID[0]==0 && Pk.SessionID[1]==0 && Pk.SessionID[2]==0  && Pk.SessionID[3]==0) { // Seriale di ZB non valido
//                return 0;
//            }
//            return 1;
//        }
//        
//        public String processInput(String InputLine){
//            String Replay="";String Command;
//            if (!(InputLine.startsWith("[")&& InputLine.endsWith("]"))) {
//                return "Errore";
//            }
//            Command = InputLine.substring(1, InputLine.length()-1);
//            
//            if ( Command.equals("Restart")){
//                return "Ok";
//            } else if (Command.equals("exit")) {
//                
//            }
//            
//            return Replay;
//        }
//    }
    private boolean exit = false;

    public TCPTaxiConnect() {
        exit = false;
        Conf = ConfClass.getInstance();
        Log = LogsClass.getInstance();
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {
        TCPConnectionList = new ArrayList<TaxiTCPConn>();
        try {
            if (Conf.TaxiDataCheck == 0) {
                return;
            }
            DBTaxi = DBConn.PoolTaxiDB.getConnection();
            DBTaxi.setAutoCommit(true);
            DBBB = DBConn.PoolBB.getConnection();
            DBBB.setAutoCommit(true);
        } catch (SQLException ex) {
            Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        Log.WriteLog(3, "TCP TAXI connector started.");

        while (!exit) {
//            System.out.println("TCP TAXI connector started.");
            try {
                ArrayList<zboxtaxi> ConnectedZBoxIDList;
                ConnectedZBoxIDList = GetTaxiZBoxListConnected();     // ritorna la lista dei dispositivi in connessione
                int i;
                for (i = 0; i < ConnectedZBoxIDList.size(); i++) {
                    CheckConnectionTaxi(ConnectedZBoxIDList.get(i));    // verifica che per lo specifico dispositivo esista la 
                    // connessione aperta verso il server dei taxi
                }
                for (i = 0; i < ConnectedZBoxIDList.size(); i++) {
                    CheckDataForTaxi(ConnectedZBoxIDList.get(i));       // verifica che per lo specifico dispositivo esistano dati da inviare 
                    // connessione aperta verso il server dei taxi
                }

                CheckCloseConnectionTaxi();     // verifcia se esistono dei sistemi con cui chiudere il canale TCP per timeout di connessione ZBox

            } catch (Exception e) {
                System.out.println("Error: Could not bind to port, or a connection was interrupted." + e.getMessage());

            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    void Close() {
        if (SSListener != null) {
            try {
                SSListener.close();
            } catch (IOException ex) {
                Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        exit = true;
    }

    // ritorna la lista dei dispositivi in connessione
    public ArrayList<zboxtaxi> GetTaxiZBoxListConnected() {
        ArrayList<zboxtaxi> ConnectedZBoxIDList = GetZBoxList(DBTaxi);
        return ConnectedZBoxIDList;
    }

    private ArrayList<zboxtaxi> GetZBoxList(Connection DbAdminConn) {
        Statement statement;
        ResultSet rs;
        ArrayList<zboxtaxi> A;

        try {
            // esegue la query al DB
            statement = DbAdminConn.createStatement();

            String QueryString = "select IDzbox,IDBlackBox, BBSerial, active,LastIDZBLocalizationSent, LastIDZBEventSent, ConnIPStart, ConnPortStart from Zbox WHERE active=1";

            rs = statement.executeQuery(QueryString);
            A = new ArrayList<zboxtaxi>();
            while (rs.next()) {
                zboxtaxi ZB = new zboxtaxi();
                ZB.IDZBox = rs.getInt("IDZBox");
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
                ZB.SerialN = Hex.decode(rs.getString("BBSerial"));
                ZB.LastIDZBLocalizationSent = rs.getLong("LastIDZBLocalizationSent");
                ZB.LastIDZBEventSent = rs.getLong("LastIDZBEventSent");
                try {
//                    ZB.ConnIPStart = InetAddress.getByName(rs.getString("ConnIPStart"));
                    ZB.ConnIPStart = InetAddress.getLocalHost();
                } catch (UnknownHostException ex) {
                    Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
                }
                ZB.ConnPortStart = rs.getInt("ConnPortStart");

                A.add(ZB);
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        return A;
    }

    // verifica che per lo specifico dispositivo esista la 
    // connessione aperta verso il server dei taxi
    public boolean CheckConnectionTaxi(zboxtaxi ConnectedZBoxID) {
        Connection DbBB = DBBB;
        Statement statement;
        ResultSet rs;
        try {
            // esegue la query al DB
            String QueryString = "select IDBlackBox, BBSerial, LastContact from BlackBox \n\r"
                    + " WHERE IDBlackBox=" + ConnectedZBoxID.IDBlackBox + ";";

            statement = DbBB.prepareStatement(QueryString);
            rs = statement.executeQuery(QueryString);
            if (rs.next()) {
                java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                ConnectedZBoxID.LastContact = rs.getTimestamp("Lastcontact");
                rs.close();
                statement.close();
                if (now1.getTime() - ConnectedZBoxID.LastContact.getTime() > 60000) {
                    return false;
                } else {
                    int i;
                    int FoundI = -1;
                    for (i = 0; i < TCPConnectionList.size(); i++) {
                        if (TCPConnectionList.get(i).ZB.IDZBox == ConnectedZBoxID.IDZBox) {
                            FoundI = i;
                            continue;
                        }
                    }
                    if (FoundI == -1) {     // ZBox non attiva ma si è attivata
                        ActivateTCPConnection(ConnectedZBoxID);
                        return true;
                    } else {
                        return true;
                    }
                }
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);

        }
        return false;
    }

    private int ActivateTCPConnection(zboxtaxi ConnectedZBoxID) {
        TaxiTCPConn TC = new TaxiTCPConn();
        try {
            TC.ClientSocket = new Socket(Conf.TaxiServerIP, Conf.TaxiServerPort, ConnectedZBoxID.ConnIPStart, ConnectedZBoxID.ConnPortStart);
            TC.in = new BufferedReader(new InputStreamReader(TC.ClientSocket.getInputStream()));
            TC.out = new BufferedOutputStream(TC.ClientSocket.getOutputStream());
            TC.ZB = ConnectedZBoxID;
            TC.StartReceiver();

            TCPConnectionList.add(TC);
            System.out.println("Aperta sessione TCP con IP=" + Conf.TaxiServerIP + " porta " + Conf.TaxiServerPort + " ZBID=" + ConnectedZBoxID.IDBlackBox
                    + " From IP " + ConnectedZBoxID.ConnIPStart.getHostAddress() + " Port " + ConnectedZBoxID.ConnPortStart);
            Log.WriteLog(3, "Aperta sessione TCP con IP=" + Conf.TaxiServerIP + " porta " + Conf.TaxiServerPort + " ZBID=" + ConnectedZBoxID.IDBlackBox
                    + " From IP " + ConnectedZBoxID.ConnIPStart.getHostAddress() + " Port " + ConnectedZBoxID.ConnPortStart);

            int i;
            int FoundI = -1;
            for (i = 0; i < TCPConnectionList.size(); i++) {
                if (TCPConnectionList.get(i).ZB.IDZBox == ConnectedZBoxID.IDZBox) {
                    FoundI = i;
                    continue;
                }
            }
            return FoundI;
        } catch (UnknownHostException ex) {
            Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        } catch (IOException ex) {
            Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }
    }

    // verifica che per lo specifico dispositivo esistano dati da inviare 
    // connessione aperta verso il server dei taxi
    public boolean CheckDataForTaxi(zboxtaxi ConnectedZBoxID) {
        // TODO: verifica che ci siano dei dati da inviare per lo specifico dispositivo ed eventualmente li invia
        Statement statement;
        ResultSet rs;
        ArrayList<zboxtaxi> A;
        int Error = 0;

        int i;
        int FoundI = -1;
        for (i = 0; i < TCPConnectionList.size(); i++) {
            if (TCPConnectionList.get(i).ZB.IDZBox == ConnectedZBoxID.IDZBox) {
                FoundI = i;
                continue;
            }
        }

        try {
            // esegue la query al DB
            statement = DBTaxi.createStatement();

            String QueryString = "SELECT IDFromZB,IDBlackBox,Data,ReceiveTimeStamp,TransmitTimeStamp,stato FROM taxi.fromzb "
                    + "where stato=0 and IDBlackBox=" + ConnectedZBoxID.IDBlackBox;

            rs = statement.executeQuery(QueryString);

            while (rs.next()) {
                if (FoundI == -1) {     // ZBox non attiva ma si è attivata
                    FoundI = ActivateTCPConnection(ConnectedZBoxID);
                }
                if (!TCPConnectionList.get(FoundI).ClientSocket.isConnected()) {
                    TCPConnectionList.remove(FoundI);
                    FoundI = ActivateTCPConnection(ConnectedZBoxID);
                }
                TaxiPack TP = new TaxiPack();

                byte[] Buffer = TP.SendReceiveReply(ConnectedZBoxID.SerialN, rs.getBytes("Data"));

                try {
                    TCPConnectionList.get(FoundI).CurrentPack = null;
                    TCPConnectionList.get(FoundI).out.write(Buffer, 0, Buffer.length);
                    TCPConnectionList.get(FoundI).out.flush();
                    java.sql.Timestamp Now = new java.sql.Timestamp((new java.util.Date()).getTime());
                    while (TCPConnectionList.get(FoundI).CurrentPack == null) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                            Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        java.sql.Timestamp Now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
                        if (Now1.getTime() - Now.getTime() > 5000) {
                            System.out.println("Comunication TIMEOUT");
                            break;
                        }
                    }
                } catch (IOException ex) {
                    Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
                    Error++;
                    continue;
                }
                UpdateStatoFromZb(rs.getLong("IDFromZB"));
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            Error++;
        }


//        try {
//            // esegue la query al DB
//            statement = DBBB.createStatement();
//
//            String QueryString = "SELECT * FROM blackbox_debug.zblocalization "
//                    + " where IDZBLocalization>"+ConnectedZBoxID.LastIDZBLocalizationSent+" and IDBlackBox="+ConnectedZBoxID.IDBlackBox
//                    + " order by IDZBLocalization Limit 100 ";
//
//            rs = statement.executeQuery(QueryString);
//            int count=0;
//
//            while (rs.next()) {
//                if(FoundI==-1) {     // ZBox non attiva ma si è attivata
//                    FoundI=ActivateTCPConnection(ConnectedZBoxID);
//                }
//                if (!TCPConnectionList.get(FoundI).ClientSocket.isConnected()) {
//                    TCPConnectionList.remove(FoundI);
//                    FoundI=ActivateTCPConnection(ConnectedZBoxID);
//                }
//                TaxiPack TP= new TaxiPack();
//                
//                byte [] Buffer=TP.SendLocalization(ConnectedZBoxID.SerialN, (byte) rs.getInt("StatoZB"),rs.getTimestamp("Btimestamp"), rs.getDouble("Blat"),rs.getDouble("Blong"));
//                ConnectedZBoxID.LastIDZBLocalizationSent=rs.getInt("IDZBLocalization");
//                
//                try {
//                    System.out.println("Send Buffer Count="+count);
//                    TCPConnectionList.get(FoundI).CurrentPack=null;
//                    TCPConnectionList.get(FoundI).out.write(Buffer, 0, Buffer.length);
//                    TCPConnectionList.get(FoundI).out.flush();
//                    System.out.println("Send Buffer ok "+count);
//                    java.sql.Timestamp Now=new java.sql.Timestamp((new java.util.Date()).getTime());
//                    while (TCPConnectionList.get(FoundI).CurrentPack==null) {
//                        try { Thread.sleep(100);
//                        } catch (InterruptedException ex) {  Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//                        }
//                        java.sql.Timestamp Now1=new java.sql.Timestamp((new java.util.Date()).getTime());
//                        if (Now1.getTime()-Now.getTime()>5000) {
//                            System.out.println("Comunication TIMEOUT");
//                            break;
//                        } 
//                            
//                    }
//                } catch (IOException ex) {
//                    ex.printStackTrace();
//                    Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//                    Error++;
//                    continue;
//                }
//                try {    Thread.sleep(50);
//                } catch (InterruptedException ex) {
//                    Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//                }
//                count ++;
//                if (count>0)
//                    UpdateLastLocEvFromZb(ConnectedZBoxID.IDBlackBox,ConnectedZBoxID.LastIDZBLocalizationSent,ConnectedZBoxID.LastIDZBEventSent);
//            }
//            
//            rs.close();
//            statement.close();
//        } catch (SQLException ex) {
//            Log.WriteEx(DBAdminClass.class.getName(), ex);
//            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            Error++;
//        }

//        try {
//            // esegue la query al DB
//            statement = DBBB.createStatement();
//
//            String QueryString = "SELECT * FROM blackbox_debug.zbEvents "
//                    + " where IDZBEvents>"+ConnectedZBoxID.LastIDZBEventSent+" and IDBlackBox="+ConnectedZBoxID.IDBlackBox
//                    + " order by IDZBEvents Limit 100 ";
//
//            rs = statement.executeQuery(QueryString);
//            int count=0;
//
//            while (rs.next()) {
//                if(FoundI==-1) {     // ZBox non attiva ma si è attivata
//                    FoundI=ActivateTCPConnection(ConnectedZBoxID);
//                }
//                if (!TCPConnectionList.get(FoundI).ClientSocket.isConnected()) {
//                    TCPConnectionList.remove(FoundI);
//                    FoundI=ActivateTCPConnection(ConnectedZBoxID);
//                }
//                TaxiPack TP= new TaxiPack();
//                
//                byte [] Buffer=TP.SendEvent(ConnectedZBoxID.SerialN, rs.getInt("IDZBEvents"),
//                        (byte) rs.getInt("IDType"),rs.getTimestamp("Btimestamp"), rs.getDouble("Blat"),rs.getDouble("Blong"),
//                        rs.getBytes("extra"));
//                ConnectedZBoxID.LastIDZBEventSent=rs.getInt("IDZBEvents");
//                
//                try {
//                    System.out.println("Send Buffer Event Count="+count);
//                    TCPConnectionList.get(FoundI).CurrentPack=null;
//                    TCPConnectionList.get(FoundI).out.write(Buffer, 0, Buffer.length);
//                    TCPConnectionList.get(FoundI).out.flush();
//                    java.sql.Timestamp Now=new java.sql.Timestamp((new java.util.Date()).getTime());
//                    while (TCPConnectionList.get(FoundI).CurrentPack==null) {
//                        try { Thread.sleep(100);
//                        } catch (InterruptedException ex) {  Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//                        }
//                        java.sql.Timestamp Now1=new java.sql.Timestamp((new java.util.Date()).getTime());
//                        if (Now1.getTime()-Now.getTime()>5000) {
//                            System.out.println("Comunication TIMEOUT");
//                            break;
//                        }
//                    }
//                } catch (IOException ex) {
//                    Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//                    Error++;
//                    continue;
//                }
//                try {    Thread.sleep(50);
//                } catch (InterruptedException ex) {
//                    Logger.getLogger(TCPTaxiConnect.class.getName()).log(Level.SEVERE, null, ex);
//                }
//                count ++;
//            if (count>0)
//                UpdateLastLocEvFromZb(ConnectedZBoxID.IDBlackBox,ConnectedZBoxID.LastIDZBLocalizationSent,ConnectedZBoxID.LastIDZBEventSent);
//            }
//            
//            rs.close();
//            statement.close();
//        } catch (SQLException ex) {
//            Log.WriteEx(DBAdminClass.class.getName(), ex);
//            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            Error++;
//        }


        if (Error == 0) {
            return true;
        } else {
            return false;
        }
    }

    public boolean UpdateStatoFromZb(long IDFromZB) {
        PreparedStatement statement;

        try {
            String QueryString = "UPDATE fromzb SET Stato=1 WHERE IDFromZB=?";

            statement = DBTaxi.prepareStatement(QueryString);
            statement.setLong(1, IDFromZB);
            statement.execute();
            statement.close();

            return true;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            return false;
        }
    }

    public boolean UpdateLastLocEvFromZb(long IDBlackBox, long LastIDZBLocalizationSent, long LastIDZBEventSent) {
        PreparedStatement statement;

        try {
            String QueryString = "UPDATE taxi.ZBox SET LastIDZBEventSent=?, LastIDZBLocalizationSent=? WHERE IDBlackBox=?";

            statement = DBTaxi.prepareStatement(QueryString);
            statement.setLong(1, LastIDZBEventSent);
            statement.setLong(2, LastIDZBLocalizationSent);
            statement.setLong(3, IDBlackBox);
            statement.execute();
            statement.close();

            return true;
        } catch (SQLException ex) {
            Log.WriteEx(DBAdminClass.class.getName(), ex);
            return false;
        }
    }

    // verifcia se esistono dei sistemi con cui chiudere il canale TCP per timeout di connessione ZBox
    public boolean CheckCloseConnectionTaxi() {
        // TODO: verifica che i canali aperti sinano tutti da dispositivi operativi da almeno 1 minuto
        return true;
    }
}
