
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class kafkaProducer {
    private static Producer<String, String> producer;

    static  {
        Properties props = new Properties();
        props.put("bootstrap.servers", "host:port");
        props.put("acks", "1");  //"0" -No ack, "1" only Leader ,"all" ALL
        props.put("retries", 0);  // "0" doesn't re try ; positive value will retry
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");
        //props.put("batch.size", 16384);  // batch size in bytes "0" means no batching

        producer = new KafkaProducer<>(props);

        // shutdown listener

        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close()));
    }


        public static void send(String key,String message)
        {
            producer.send(new ProducerRecord<>("tweets",key,message)); //tweets = previous topic
            System.out.println(key+":"+message);
        }


    }


