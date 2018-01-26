/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.AsyncKafkaTemplate.RequestReplyFuture;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Gary Russell
 * @since 2.1.3
 *
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class AsyncKafkaTemplateTests {

	private static final String A_REPLY = "aReply";

	private static final String A_REQUEST = "aRequest";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, A_REQUEST, A_REPLY);

	private static ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();

	@Autowired
	private Config config;

	@BeforeClass
	public static void setUp() {
		scheduler.initialize();
	}

	@AfterClass
	public static void tearDown() {
		scheduler.shutdown();
	}

	@Test
	public void testGood() throws Exception {
		AsyncKafkaTemplate<Integer, String, String> template = createTemplate();
		template.setReplyTimeout(30_000);
		ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(A_REQUEST, "foo");
		RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
		future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
		ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
		assertThat(consumerRecord.value()).isEqualTo("FOO");
		template.stop();
	}

	@Test
	public void testTimeout() throws Exception {
		AsyncKafkaTemplate<Integer, String, String> template = createTemplate();
		template.setReplyTimeout(1);
		ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(A_REQUEST, "foo");
		RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
		future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
		try {
			future.get(30, TimeUnit.SECONDS);
			fail("Expected Exception");
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		}
		catch (ExecutionException e) {
			assertThat(e).hasCauseExactlyInstanceOf(KafkaException.class).hasMessageContaining("Reply timed out");
		}
		template.stop();
	}

	public AsyncKafkaTemplate<Integer, String, String> createTemplate() {
		ContainerProperties containerProperties = new ContainerProperties(A_REPLY);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("reqResp", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProperties);
		AsyncKafkaTemplate<Integer, String, String> template = new AsyncKafkaTemplate<>(this.config.pf(), container);
		scheduler.initialize();
		template.setTaskScheduler(scheduler);
		template.start();
		return template;
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public DefaultKafkaProducerFactory<Integer, String> pf() {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
			return pf;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> cf() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("serverSide", "false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
			return cf;
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<>(pf());
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf());
			factory.setReplyTemplate(template());
			return factory;
		}

		@KafkaListener(topics = A_REQUEST)
		@SendTo(A_REPLY)
		public String handle(String in) {
			return in.toUpperCase();
		}

	}

}
