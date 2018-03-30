/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import com.viacom.DB.DBAdminClass;
import com.viacom.zbox.PushNotifyEvent;
import com.viacom.zbox.zbox;
import java.io.IOException;
import java.net.*;
import java.security.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;

/**
 *
 * @author Luca
 */
public class PushNotifyThread {

    DBAdminClass DbAdmin;

    public boolean CheckPushNotifyEvents(DBAdminClass DbAdm) {
        DbAdmin = DbAdm;
        byte[] sendData = new byte[1024];

        // richiedo la lista dei veicoli da notificare
        ArrayList<PushNotifyEvent> PNE = GetPushList();
        // invio notifica
        if (PNE.size() > 0) {
            for (int i = 0; i < PNE.size(); i++) {
                System.out.println("Invio PushNotify a ZB : " + PNE.get(i).SerialN);
                // invia la comunicazione
                try {
                    byte[] buff = new byte[16];
                    sendData = SendGPRSData(PNE.get(i).ZB, (byte) 0, ZB_UDP_Comunication.GPRS_COMMAND_PUSH_NOTIFY, buff, (byte) 0x01);
                    DatagramSocket clientSocket = new DatagramSocket();
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, PNE.get(i).IP, PNE.get(i).Port);
                    clientSocket.send(sendPacket);
                } catch (SocketException ex) {
                    Logger.getLogger(PushNotifyThread.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(PushNotifyThread.class.getName()).log(Level.SEVERE, null, ex);
                }

                // aggiorna il DB
                //DbAdm.SetPushNotifyStatus(PNE.get(i).IDBlackBox, 1);

                // aggiorna il DB
                //DbAdm.SetPushNotifyStatus(PNE.get(i).IDBlackBox, 1);
            }
        }
        return true;
    }

    ArrayList<PushNotifyEvent> GetPushList() {
        ArrayList<PushNotifyEvent> PNE = null;

        PreparedStatement statement, statement1;
        ResultSet rs;

        DbAdmin.CheckConnection();
        try {
            if (DbAdmin.DbAdminConn.isClosed()) {
                return PNE;
            }
        } catch (SQLException ex) {
            return PNE;
        }
        try {
            DbAdmin.DbAdminConn.setAutoCommit(true);

            // esegue la query al DB
            String QueryString = "SELECT PN.IDZBPushNotify, PN.IDBlackBox, PN.Stato, PN.LastSent, "
                    + "BB.BBSerial, BB.IP, BB.Port, BB.LastContact "
                    + "FROM ZBPushNotify PN "
                    + "LEFT JOIN BlackBox BB ON BB.IDBlackBox=PN.IDBlackBox "
                    + "WHERE Stato=0";
            statement = DbAdmin.DbAdminConn.prepareStatement(QueryString);

            rs = statement.executeQuery(QueryString);
            PNE = new ArrayList<PushNotifyEvent>();
            while (rs.next()) {
                try {
                    PushNotifyEvent PN = new PushNotifyEvent();
                    PN.IDZBPushNotify = rs.getInt("IDZBPushNotify");
                    PN.IDBlackBox = rs.getInt("IDBlackBox");
                    PN.LastPushSent = rs.getTimestamp("LastSent");
                    PN.SerialN = rs.getString("BBSerial");

                    PN.IP = InetAddress.getByName(rs.getString("IP"));

                    PN.Port = rs.getInt("Port");
                    PN.LastContact = rs.getTimestamp("LastContact");
                    PN.ZB = DbAdmin.GetZBox(PN.IDBlackBox);
                    PNE.add(PN);
                } catch (UnknownHostException ex) {
                    Logger.getLogger(PushNotifyThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }


        return PNE;
    }

    /**
     * Invia un comando in GPRS alla ZB
     *
     * @param ZB ZBox a cui inviare i dati
     * @param ServerNum ---- non usato
     * @param type Tipo di pacchetto da inviare
     * @param data payload
     * @param Crypt 1 se da cifrare, 0 se da inviare in chiaro
     * @return
     */
    byte[] SendGPRSData(zbox ZB, byte ServerNum, byte type, byte[] data, byte Crypt) {
        byte[] buff = new byte[data.length + 13];
//	DPRINT("PollGPRS SendGPRSData PayloadSize=%d\n\r",data.length);
        int i = 0;
        byte Size = (byte) (data.length >> 4);
        byte Fill;
        byte Resto = (byte) (data.length & 0xF);
        if (Resto != 0) {
            Size++;
        }
        Fill = (byte) ((Size << 4) - data.length);
        System.out.println("SendGPRSData Size=" + Size + " Fill=" + Fill);

        buff[i] = (byte) 0xA5;
        i++;								// SOM 1
        buff[i] = (byte) 0x5A;
        i++;								// SOM 2

        System.arraycopy(ZB.SerialN, 0, buff, 2, 4);

        i += 4;
        ZB.PackNRef++;
        buff[i] = (byte) ZB.PackNRef;
        i++;                                                  // PackN
        buff[i] = type;
        i++;								// Type
        buff[i] = Size;
        i++;								// size
        buff[i] = (byte) ((Crypt << 7) + Fill);
        i++;            				// Crypt/Fill
        if (Crypt > 0) {
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

            SecretKeySpec key = new SecretKeySpec(ZB.AESKeyIn, "AES");
            try {
                Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
                try {
                    byte[] b;
                    cipher.init(Cipher.ENCRYPT_MODE, key);
                    b = cipher.doFinal(data);
//                    System.out.println("SendGPRSData data.lenght="+data.length+":"+(new String(Hex.encode(data))));
//                    System.out.println("SendGPRSData buff.lenght="+buff.length+":"+(new String(Hex.encode(buff))));
//                    System.out.println("SendGPRSData b.lenght="+b.length+":"+(new String(Hex.encode(b))));
                    System.arraycopy(b, 0, buff, 10, b.length);
                } catch (IllegalBlockSizeException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                } catch (BadPaddingException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InvalidKeyException ex) {
                    Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (NoSuchAlgorithmException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchProviderException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchPaddingException ex) {
                Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {
            System.arraycopy(data, 0, buff, i, data.length);        			// Payload
        }
        i += data.length;
        i += Fill;

        int nByte = i;
        System.out.println("SendGPRSData nByte=" + nByte + " i=" + i);

        MessageDigest cript;

        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(buff, 0, nByte);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(ZB_UDP_Comunication.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

        byte[] CRC = cript.digest();

        buff[i] = CRC[0];
        i++;
        buff[i] = CRC[1];
        i++;
        buff[i] = CRC[2];
        i++;
        System.out.println("SendGPRSData data= " + (new String(Hex.encode(buff))));
        return buff;
    }
}
