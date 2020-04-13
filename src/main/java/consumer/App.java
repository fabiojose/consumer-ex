package consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
    public static void main(String[] args) {
        
        System.out.println(" > > Consumer java");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", "earliest");

        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        // #########
        // Grupo de consumo
        props.put("group.id", "kafka-br");

        try(KafkaConsumer<String, String> consumer = 
                new KafkaConsumer<>(props)){

            // #########
            // Tópico
            consumer.subscribe(Arrays.asList("topico-a"), new SimpleListener());

            while(true){
                ConsumerRecords<String, String> records = 
                    consumer.poll(Duration.ofSeconds(5));

                if(records.count() > 0){
                    System.out.println(" > > tamanho do lote consumido: " 
                        + records.count());
                }
                
                // Processar os registros
                records
                    .forEach(record -> {
                        System.out.println(" > > Partição: " + record.partition() 
                            + ", Offset: " + record.offset() 
                            + ", valor: " + record.value());
                    });

                // #########
                // Commit do maior offset recebido
                consumer.commitSync();
            }
        }
    }
}
