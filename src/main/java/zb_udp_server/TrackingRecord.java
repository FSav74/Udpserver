/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 *
 * @author Luca
 */
public class TrackingRecord {

    public String IDObu;
    public int IDGuida;
    public double Lat;
    public double Long;
    public int ValidGPS;
    public Timestamp Tempo;
    public int velocita;    // espressa in km/h
    public int bearing;
    public long odometro;
    public int fuel;
    public int StatoZB;

    String getString() {
        SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        String ret = "";
        ret += IDObu + ",";
        ret += IDGuida + ",";
        ret += Lat + ",";
        ret += Long + ",";
        ret += FullDateFormat.format(Tempo) + ",";
        ret += velocita + ",";
        ret += bearing + ",";
        ret += odometro + ",";
        ret += fuel + ",";
        ret += StatoZB + ",";
        ret += ValidGPS + "\n";


        return ret;
    }
}
