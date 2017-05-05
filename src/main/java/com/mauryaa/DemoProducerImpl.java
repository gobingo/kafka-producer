package com.mauryaa;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DemoProducerImpl implements DemoProducer {

  private String topic;
  private Properties kafkaProps = new Properties();
  private Producer<String, String> producer;

  public DemoProducerImpl(String topic) {
    this.topic = topic;
  }

  @Override
  public void configure(String brokerList) {
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
    kafkaProps.put(ProducerConfig.RETRIES_CONFIG, "3");
    kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
  }

  @Override
  public void start() {
    producer = new KafkaProducer<String, String>(kafkaProps);
  }

  @Override
  public void produce(String value) throws ExecutionException, InterruptedException {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
    producer.send(record, new DemoProducerCallback());
  }

  @Override
  public void close() {
    producer.close();
  }

  private class DemoProducerCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        System.out.println("Error producing to topic " + recordMetadata.topic());
        e.printStackTrace();
      }
    }
  }

}
