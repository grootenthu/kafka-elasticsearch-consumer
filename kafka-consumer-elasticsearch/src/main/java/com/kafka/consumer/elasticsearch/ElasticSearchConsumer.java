package com.kafka.consumer.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	public static void main(String[] args) throws IOException {
		final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
		logger.info("Start Twitter & Elastic Search Integration");
		RestHighLevelClient client = createClient();
		KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_tweets");

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			logger.info("Received a batch of : " + records.count() + " records");
			
			BulkRequest bulkRequest = new BulkRequest();
			for (ConsumerRecord<String, String> record : records) {
				IndexRequest indexRequest;
				try {
					indexRequest = new IndexRequest("PUT", "twitter/tweets", extractIdFromJson(record))
					.source(record.value(), XContentType.JSON);
					
					bulkRequest.add(indexRequest);
				} catch (NullPointerException e) {
					logger.warn("Skipping bad data : " + record.value());
				}
			}
			if (records.count() > 0) {
				BulkResponse response = executeRequest(client, bulkRequest);
				logger.info("Took " + response.getTook().getSeconds() + " secs to process batch");

				logger.info("Committing offsets");
				consumer.commitSync();
				logger.info("Offsets have been committed");
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		// client.close();
	}

	private static BulkResponse executeRequest(RestHighLevelClient client, BulkRequest bulkRequest)
			throws IOException {
		
		return client.bulk(bulkRequest, RequestOptions.DEFAULT);
	}

	private static String extractIdFromJson(ConsumerRecord<String, String> record) {
		// There can 2 strategies to make any consumer idempotent
		// 1 -> Use kafka generic ID
		// String id = record.topic()+record.partition()+record.offset();

		// 2 -> Use twitter / API specific ID
		// we use option 2 which is as follows
		String id = JsonParser.parseString(record.value())
							  .getAsJsonObject()
							  .get("id_str")
							  .getAsString();
		return id;
	}

	private static KafkaConsumer<String, String> createKafkaConsumer(String topicName) {
		String bootstrapServers = "localhost:9092";
		String groupId = "kafka-elastic-search";

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		//Building capability for commiting offsets manually
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topicName));

		return consumer;
	}

	public static RestHighLevelClient createClient() {

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("1cv9ii5few", "m09dlhmfta"));

		RestClientBuilder builder = RestClient
				.builder(new HttpHost("kafka-twitter-6047828393.ap-southeast-2.bonsaisearch.net", 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		
		return client;
	}

}
