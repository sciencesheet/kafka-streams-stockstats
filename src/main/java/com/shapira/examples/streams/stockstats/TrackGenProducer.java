package com.shapira.examples.streams.stockstats;

import com.shapira.examples.streams.stockstats.serde.JsonSerializer;
import com.shapira.examples.streams.stockstats.model.Track;
import com.shapira.examples.streams.stockstats.model.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;


public class TrackGenProducer {

    public static KafkaProducer<String, Track> producer = null;

    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

        System.out.println("Press CTRL-C to stop generating data");


        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting Down");
                if (producer != null)
                    producer.close();
            }
        });

        JsonSerializer<Trade> tradeSerializer = new JsonSerializer<>();

        // Configuring producer
        Properties props;
        if (args.length==1)
            props = LoadConfigs.loadConfig(args[0]);
        else
            props = LoadConfigs.loadConfig();

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", tradeSerializer.getClass().getName());

        // Starting producer
        producer = new KafkaProducer<>(props);


        // initialize

        Random random = new Random();
        long iter = 0;
        
        
        BufferedReader reader = new BufferedReader(new FileReader(new File("sample.csv")));
        String line = null;
        Map<String, String> map = new HashMap<String, String>();// it should be static - whereever you define
        while ((line = reader.readLine()) != null) {
            if (line.contains("\\,")) {
                String[] strings = line.split("\\,");
                map.put(strings[0], strings[1]);
            }
        }
        reader.close();
      

        // Start generating events, stop when CTRL-C

        while (true) {
            iter++;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String number = entry.getKey();
                String date = entry.getValue();

                Track track = new Track(number,date);
                
                // Note that we are using ticker as the key - so all asks for same stock will be in same partition
                ProducerRecord<String, Track> record = new ProducerRecord<>(map.get(number), track);

                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing events");
                        e.printStackTrace();
                    }
                });

                // Sleep a bit, otherwise it is frying my machine
                Thread.sleep(Constants.DELAY);
            }
        }
    }
}
