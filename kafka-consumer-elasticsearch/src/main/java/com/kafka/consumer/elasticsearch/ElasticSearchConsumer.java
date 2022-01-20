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
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

	public static void main(String[] args) throws IOException {
		final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
		logger.info("Start Twitter & Elastic Search Integration");
		RestClient client = createClient();
		KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_tweets");

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				Response response = executeRequest(client, record);
				logger.info(EntityUtils.toString(response.getEntity()));

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		// client.close();
	}

	private static Response executeRequest(RestClient client, ConsumerRecord<String, String> record)
			throws IOException {
		Request request = new Request("POST", "/twitter/tweets");

		request.setJsonEntity(record.value());
		Response response = client.performRequest(request);
		return response;
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

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topicName));

		return consumer;
	}

	public static RestClient createClient() {

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("1cv9ii5few", "m09dlhmfta"));

		return RestClient
				.builder(new HttpHost("kafka-twitter-6047828393.ap-southeast-2.bonsaisearch.net", 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				}).build();
	}

}
