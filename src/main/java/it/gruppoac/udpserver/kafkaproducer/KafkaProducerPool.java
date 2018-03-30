/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.gruppoac.udpserver.kafkaproducer;

import com.edi.kafka.messages.MessageEncoder;
import com.edi.kafka.messages.MessageSenderPool;
import com.edi.kafka.messages.MessageSender;
import com.edi.kafka.messages.exception.ProducerException;
import java.util.Properties;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.viacom.zbox.ZBRecord;
import java.io.InputStream;
import zb_udp_server.LogsClass;

/**
 *
 * @author Admin
 */
public enum KafkaProducerPool {
    INSTANCE;

    private Properties properties = null;
    private MessageSenderPool pool = null;
    private MessageEncoder<ZBRecord> stringEncoder = null;
    
    private LogsClass logger;
    
    private KafkaProducerPool(){

        //TODO:-----------------------------------------------------------------
        //TODO: 
        //TODO: Il recupero delle properties è effettuato
        //nel bat di lancio: con l' aggiunta nel classpath la cartella properties dove è
        //presente producer.props
        //TODO:-----------------------------------------------------------------
        try {
            InputStream props = Resources.getResource("producer.props").openStream();
            System.out.println("KafkaProducerPool - constructor");
            properties = new Properties();
            properties.load(props);                     
            String brokerList = properties.getProperty("bootstrap.servers");
            System.out.println("BROKER LIST:"+brokerList);
            String queueName = properties.getProperty("AC_QUEUE_NAME");
            String maxConnection = properties.getProperty("AC_MAX_POOL");
            
            logger.getInstance();
            
            int maxConnectionI = 150;
            try{
                maxConnectionI=Integer.parseInt(maxConnection);
            }catch(Exception e){
                logger.WriteLog(1, "Errore configurazione parametro AC_MAX_POOL: lascio default");
            }
            
            pool = new MessageSenderPool(maxConnectionI, queueName, brokerList, properties);
            
            
            stringEncoder = new MessageEncoder<ZBRecord>() {

				@Override
				public String encodeMessage(ZBRecord record) {
					Gson gson = new Gson();
                                        String jsonInString = gson.toJson(record);
					return jsonInString;
				}
				
			};
            
            
        }catch(Exception e){
            	e.printStackTrace();
                //TODO: rivedere gestione errori: qui viene lanciata una runTimeException
                throw new RuntimeException("Errore caricamento file Properties.",e);
        }
    }
   
    public void sendMessage(ZBRecord record) throws ProducerException{
         MessageSender sender = pool.getSender(5000);
         sender.send(record, stringEncoder);
         sender.close();
         return;
    }
            
  /*  private KafkaProducerPool(){
        //TODO:-----------------------------------------------------------------
        //TODO: 
        //TODO: Il recupero delle properties è effettuato
        //nel bat di lancio: con l' aggiunta nel classpath la cartella properties dove è
        //presente producer.props
        //TODO:-----------------------------------------------------------------
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            System.out.println("SimpleProducer constructor");
            properties = new Properties();
            properties.load(props);
            int THREAD_COUNT = 5;
            threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
            
        }catch(Exception e){
            	e.printStackTrace();
                //TODO: rivedere gestione errori: qui viene lanciata una runTimeException
                throw new RuntimeException("Errore caricamento file Properties.",e);
        }
    }
    
    
    public void sendMessage(String message){
        threadPool.execute(new ProducerThread(message));
    }
}*/
    
}
