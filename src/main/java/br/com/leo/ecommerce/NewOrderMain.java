package br.com.leo.ecommerce;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var dispatcher = new KafkaDispatcher();

       var producer =  new KafkaProducer<String, String>(properties());

       for (var i =0; i<10; i++){

           var key = UUID.randomUUID().toString();
           var value = key+"131232,321163,56466456";

           dispatcher.send("ECOMMERCE_NEW_ORDER", key, value );

           var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value );

           Callback callback = (data, e) -> {
               if (e != null) {
                   e.printStackTrace();
                   return;
               }
               System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
           };

           var email = "Thank you for your order";
           var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email );
           producer.send(record, callback).get();
           producer.send(emailRecord, callback).get();
       }
    }


}
