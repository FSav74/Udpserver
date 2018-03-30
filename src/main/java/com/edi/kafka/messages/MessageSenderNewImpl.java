/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.edi.kafka.messages;

import com.edi.kafka.messages.exception.ProducerException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author Admin
 */
public class MessageSenderNewImpl implements MessageSender{

    
    private KafkaProducer<String, String> producer;
    
    //private Producer<String, byte[]> producer;
	private String topic;
	private MessageSenderPool pool;
        
    public MessageSenderNewImpl(Properties props, String topic, MessageSenderPool pool) {
        
        try{
                //super();
                String list = props.getProperty("bootstrap.servers");
                //System.out.println("LISTA:"+list);
                //System.out.println("topic:"+topic);
		producer = new KafkaProducer<>(pool.getMyProperties());
                
		this.topic = topic;
		this.pool = pool;
        }catch(Throwable t){
            System.out.println("T:"+t.getMessage());
            
        }
	}

    
        public String getTopic(){
            return topic;
        }
        public MessageSenderPool getPool(){
            return pool;
        }
    
	public void send(byte[] msg) {
		//KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic, msg);
		//this.producer.send(data);
	}
	
	public <T>  void send(T msg, MessageEncoder<T> decoder) throws ProducerException
	{
                
          
                //System.out.println("send mesage");
                //System.out.println("topic:"+this.topic);
                
                 try {
          
                     String messageToSend = decoder.encodeMessage(msg);
                     //System.out.println("messageToSend:"+messageToSend);
             RecordMetadata recordMetaData = producer.send(new ProducerRecord<String, String>(
                        this.topic,
                        messageToSend)
            ).get();
             
            //System.out.println("response mesage");
            //il .get() rende asincrono il meto: si aspetta la replica del server.
            
            //producer.flush();
            //----------------------------------------------------------------
            //il metodo get è quello che mi fa passare alla chiamata synchrona 
            //synchronous 
            // e posso recuperare l'esito dell'accodamento
            //
            //---------------------------------------------------------------
            //System.out.println("Message produced, offset: " + recordMetaData.offset());
            //System.out.println("Message produced, partition: " + recordMetaData.partition());
            //System.out.println("Message produced, topic: " + recordMetaData.topic());
           
  
            
        } catch (Throwable throwable) {
             System.out.println("THROWABLE error");
            System.out.printf("%s", throwable.getStackTrace());
            throw new ProducerException(throwable);
        }
	}

	public void close() {
		this.pool.returnSender(this);
	}
	
	public void shutDown() {
		this.producer.close();
	}
    
}
