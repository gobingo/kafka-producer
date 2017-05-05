package com.mauryaa;

import java.util.concurrent.ExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerApplication {

  private static final int NUM_MESSAGES = 100;
  private static final int SLEEP_MILLIS = 1000;
  private static Configuration conf;

  @Autowired
  public void setConfiguration(Configuration conf) {
    KafkaProducerApplication.conf = conf;
  }

	public static void main(String[] args) throws ExecutionException, InterruptedException {
    SpringApplication.run(KafkaProducerApplication.class, args);

    String brokerList = conf.getBrokerList();
    String topic = conf.getTopic();

    DemoProducer producer = new DemoProducerImpl(topic);
    producer.configure(brokerList);
    producer.start();

    producer.produce("start of message batch...");

    for (int i = 0; i < NUM_MESSAGES; i++ ) {
      System.out.println("message " + i);
      producer.produce(Integer.toString(i));
      Thread.sleep(SLEEP_MILLIS);
    }

    producer.produce("end of message batch...");
    producer.close();
	}

}
