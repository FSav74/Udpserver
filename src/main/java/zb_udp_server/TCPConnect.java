/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package zb_udp_server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luca
 */
public class TCPConnect extends Thread {

    private ConfClass Conf;
    private LogsClass Log;
    ServerSocket SSListener = null;

    private class ThreadedClass extends Thread {

        Socket ClientSocket;

        private ThreadedClass(Socket connection) {
            //throw new UnsupportedOperationException("Not yet implemented");
            ClientSocket = connection;
        }

        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run() {
            PrintWriter out = null;
            try {
                out = new PrintWriter(ClientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(ClientSocket.getInputStream()));

                String inputLine, outputLine;
                // initiate conversation with client

//                out.println("HELO\n\r");
                while (!in.ready()) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(TCPConnect.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    Logger.getLogger(TCPConnect.class.getName()).log(Level.SEVERE, null, ex);
                }
                /*                while ((inputLine = in.readLine()) != null) {
                 System.out.println("Input : "+inputLine);
                 outputLine = processInput(inputLine);
                 System.out.println("Output: "+outputLine);
                 out.println(outputLine);
                 if (outputLine.equals("Bye."))
                 break;
                 }*/
                String Str = "";
                int ch_in = 0;
                while (in.ready()) {
                    ch_in = in.read();
                    if (ch_in != -1) {
                        Str += (char) ch_in;
                    }
                }
                System.out.println("INPUT(size " + Str.length() + "): " + Str);

                String OutString = "HTTP/1.1 200 OK\r\n"
                        + "Cache-Control: private, max-age=0\r\n"
                        + "Content-Type: text/xml; charset=utf-8\r\n"
                        + "X-AspNet-Version: 2.0.50727\r\n"
                        + "X-Powered-By: ASP.NET\r\n"
                        + "Date: Tue, 20 Nov 2012 13:26:47 GMT\r\n"
                        + "Content-Length: 410\r\n"
                        + "Set-Cookie: TS77dfdd=c4f67eb7c1b0419133f6158be33641b7a0401778249b6b5750ab8517; Path=/\r\n"
                        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"><soap:Body><InsertClientExtPosResponse xmlns=\"http://concerto.comune.parma.it/concerto_ws/\">"
                        + "<InsertClientExtPosResult>0</InsertClientExtPosResult></InsertClientExtPosResponse></soap:Body></soap:Envelope>\r\n";
                out.print(OutString);
                System.out.println("OUTPUT (size " + OutString.length() + "): " + OutString);
            } catch (IOException ex) {
                Logger.getLogger(TCPConnect.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                out.close();
            }
        }

        public String processInput(String InputLine) {
            String Replay = "";
            String Command;
            if (!(InputLine.startsWith("[") && InputLine.endsWith("]"))) {
                return "Errore";
            }
            Command = InputLine.substring(1, InputLine.length() - 1);

            if (Command.equals("Restart")) {
                return "Ok";
            } else if (Command.equals("exit")) {
            }

            return Replay;
        }
    }
    private boolean exit = false;

    public TCPConnect() {
        exit = false;
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {
        while (!exit) {
            try {
                System.out.println("Waiting TCP Connection");
                ServerSocket SSListener = new ServerSocket(Conf.ListenPort);
                while (true) {
                    Socket connection = SSListener.accept();
                    System.out.println("Connessione stabilita da " + connection.getInetAddress().getHostAddress());
                    ThreadedClass worker = new ThreadedClass(connection);
                    System.out.println("Processo di gestione della connessione attivato.");
                    worker.start();
                }
            } catch (Exception e) {
                System.out.println("Error: Could not bind to port, or a connection was interrupted.");
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                Logger.getLogger(TCPConnect.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    void Close() {
        if (SSListener != null) {
            try {
                SSListener.close();
            } catch (IOException ex) {
                Logger.getLogger(TCPConnect.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        exit = true;
    }

    /**
     * *
     * Imposta la classe di configurazione
     *
     * @param C1 : classe di configurazione
     * @return true se la riconfigurazinoe si è conclusa con successo
     */
    public boolean SetConf(ConfClass C1) {
        if (C1 != null) {
            Conf = C1;
            return true;
        } else {
            return false;
        }
    }

    public boolean SetLog(LogsClass L) {
        if (L != null) {
            Log = L;
            return true;
        } else {
            return false;
        }
    }
}
