package workers;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class HelloProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer producer = new KafkaProducer(props);
        String key = "key1";
        String programSchema = "{\"type\":\"record\"," +
                "\"name\":\"program\"," +
                "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(programSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", "EP01");

        ProducerRecord<Object, Object> record = new ProducerRecord<>("program.topic", key, avroRecord);
        System.out.println("Sending message "+record);
        try {
            producer.send(record);
        } catch (SerializationException e) {
            e.printStackTrace();
        }
        System.out.println("Finished ....");
    }
}
