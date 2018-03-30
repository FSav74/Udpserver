/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.edi.kafka.messages;

import com.edi.kafka.domains.ZkHosts;

/**
 *
 * @author Admin
 */
public class KafkaMessageEncoder implements MessageEncoder<ZkHosts>{

    @Override
    public String encodeMessage(ZkHosts msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
