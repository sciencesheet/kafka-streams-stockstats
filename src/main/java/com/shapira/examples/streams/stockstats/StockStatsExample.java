package com.shapira.examples.streams.stockstats;

import com.shapira.examples.streams.stockstats.serde.JsonDeserializer;

import com.shapira.examples.streams.stockstats.serde.JsonSerializer;
import com.shapira.examples.streams.stockstats.serde.WrapperSerde;
import com.shapira.examples.streams.stockstats.model.Trade;
import com.shapira.examples.streams.stockstats.model.TradeStats;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import org.springframework.http.HttpEntity;

import org.springframework.util.Base64Utils;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpMethod;

import java.util.Properties;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

//import com.google.gson.Gson;
//import com.google.gson.JsonArray;//
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;
//import com.google.gson.*;

@EnableWebSecurity
@Configuration
/**
 * Input is a stream of trades
 * Output is two streams: One with minimum and avg "ASK" price for every 10 seconds window
 * Another with the top-3 stocks with lowest minimum ask every minute
 */
public class StockStatsExample {
	
	static String upsTrackingResponseTemplate = "" +
			"{\n" +
			"\"tracking_number\" : {\n" + 
			"\"status_code\" : {\n" + 
			"\"status_description\" : {\n" + 
			"\"carrier_status_code\" : {\n" + 
			"\"carrier_status_description\" : {\n" + 
			"\"ship_date\" : {\n" + 
			"\"estimated_deliver_date\" : {\n" + 
			"\"actual_delivery_date\" : {\n" +
			"\"exception_description\" : {\n" +
			"\"events\" : [\n" +
			"{\n";
			

    public static void main(String[] args) throws Exception {
    	
    	//Gson gson = new Gson();

	String apiKey = "TEST_g3FLUKv9BCpmOmVlL/L5Yh8s+m6+y0BH99eW32hbzCs";
        // encode api token
    	byte[] xApiAuthTokenBytes = apiKey.getBytes("utf-8");
    	String xApiAuthToken = Base64.getEncoder().encodeToString(xApiAuthTokenBytes);

	final String uri = "https://api.shipengine.com/v1/tracking?carrier_code=ups&tracking_number=1Z61EA138790939573";
     
    	RestTemplate restTemplate = new RestTemplate();
     
    	HttpHeaders headers = new HttpHeaders();
    	headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
    	headers.add("api-key", apiKey);
	
    	HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);   
    	ResponseEntity<String> result = restTemplate.exchange(uri, HttpMethod.GET, entity, String.class);     
    	//System.out.println(result.toString());
    	
    	

    	JsonParser springParser = JsonParserFactory.getJsonParser();
    	Map<String, Object> map = springParser.parseMap(result.getBody());

    	String mapArray[] = new String[map.size()];
    	System.out.println("Items found: " + mapArray.length);

    	int i = 0;
    	for (Map.Entry<String, Object> entry : map.entrySet()) {
    			System.out.println(entry.getKey() + " = " + entry.getValue());
    			i++;
    	}
    
    	
    	


        Properties props;
        if (args.length==1)
            props = LoadConfigs.loadConfig(args[0]);
        else
            props = LoadConfigs.loadConfig();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat-2");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // creating an AdminClient and checking the number of brokers in the cluster, so I'll know how many replicas we want...

        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = dcr.nodes().get().size();

        if (clusterSize<3)
            props.put("replication.factor",clusterSize);
        else
            props.put("replication.factor",3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trade> source = builder.stream(Constants.STOCK_TOPIC);

        KStream<Windowed<String>, TradeStats> stats = source
                .groupByKey()
                .windowedBy(TimeWindows.of(5000).advanceBy(1000))
                .<TradeStats>aggregate(() -> new TradeStats(),(k, v, tradestats) -> tradestats.add(v),
                        Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates")
                                .withValueSerde(new TradeStatsSerde()))
                .toStream()
                .mapValues((trade) -> trade.computeAvgPrice());

        stats.to("stockstats-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println(topology.describe());

        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    static public final class TradeSerde extends WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<Trade>(), new JsonDeserializer<Trade>(Trade.class));
        }
    }

    static public final class TradeStatsSerde extends WrapperSerde<TradeStats> {
        public TradeStatsSerde() {
            super(new JsonSerializer<TradeStats>(), new JsonDeserializer<TradeStats>(TradeStats.class));
        }
    }

}
  