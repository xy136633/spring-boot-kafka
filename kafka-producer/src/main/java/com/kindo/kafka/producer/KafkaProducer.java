package com.kindo.kafka.producer;

import java.util.Date;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import com.alibaba.fastjson.JSONObject;
import com.kindo.kafka.vo.Message;

@Service
public class KafkaProducer {
	public final static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	public void send(String topic, String msg) {
		Message message = new Message();
		message.setId(System.currentTimeMillis());
		message.setMsg(msg);
		message.setSendTime(new Date());
		logger.info("+++++++++++++++++++++  message = {}", JSONObject.toJSONString(message));
		
		ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, JSONObject.toJSONString(message)); //发送消息，topic不存在将自动创建新的topic
        listenableFuture.addCallback(//添加成功发送消息的回调和失败的回调
                result -> logger.info("send message to {} success",topic),
                ex -> logger.info("send message to {} failure,error message:{}",topic,ex.getMessage()));
		
	}
}