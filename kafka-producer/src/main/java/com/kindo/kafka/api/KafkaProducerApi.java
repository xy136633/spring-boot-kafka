package com.kindo.kafka.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kindo.kafka.producer.KafkaProducer;

@RestController
@RequestMapping("/kafkaProducerApi")
public class KafkaProducerApi {
	public final static Logger logger = LoggerFactory.getLogger(KafkaProducerApi.class);

	@Autowired
    private KafkaProducer kafkaProducer;
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "/{topic}/send",method = RequestMethod.GET)
    public void sendMeessage(
            @RequestParam(value = "message",defaultValue = "hello world") String message,
            @PathVariable final String topic) {
        logger.info("start sned message to {}",topic);
        for (int i=0; i<10000; i++){
        	kafkaProducer.send(topic, message+i);
		}
    }

    @RequestMapping(value = "/default/send",method = RequestMethod.GET)
    public void sendMeessagedefault() {//发送消息到默认的topic
        logger.info("start send message to default topic");
        kafkaTemplate.sendDefault("你好，世界");
    }
}
