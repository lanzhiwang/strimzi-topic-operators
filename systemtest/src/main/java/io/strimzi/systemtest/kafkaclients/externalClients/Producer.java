/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.kafkaclients.KafkaClientProperties;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

public class Producer extends ClientHandlerBase<Integer> implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    private KafkaClientProperties properties;
    private final AtomicInteger numSent = new AtomicInteger(0);
    private final String topic;
    private String clientName;
    private final Integer partition;

    Producer(KafkaClientProperties properties, CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate,
             String topic, String clientName, Integer partition) {
        super(resultPromise, msgCntPredicate);
        this.properties = properties;
        this.partition = partition;
        this.topic = topic;
        this.clientName = clientName;
        this.vertx = Vertx.vertx();
    }

    Producer(KafkaClientProperties properties, CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate,
             String topic, String clientName) {
        super(resultPromise, msgCntPredicate);
        this.properties = properties;
        this.topic = topic;
        this.partition = null;
        this.clientName = clientName;
        this.vertx = Vertx.vertx();
    }

    @Override
    protected void handleClient() {
        LOGGER.info("Creating instance of Vert.x for the client {}", this.getClass().getName());

        LOGGER.info("Producer is starting with following properties: {}", properties.getProperties().toString());

        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, properties.getProperties());

        if (msgCntPredicate.test(-1)) {
            vertx.eventBus().consumer(clientName, msg -> {
                if (msg.body().equals("stop")) {
                    LOGGER.debug("Received stop command! Produced messages: {}", numSent.get());
                    resultPromise.complete(numSent.get());
                }
            });
            vertx.setPeriodic(1000, id -> sendNext(producer, topic));
        } else {
            sendNext(producer, topic);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing Vert.x instance for the client {}", this.getClass().getName());
        if (vertx != null) {
            vertx.close();
        }
    }

    private void sendNext(KafkaProducer<String, String> producer, String topic) {
        if (msgCntPredicate.negate().test(numSent.get())) {

            KafkaProducerRecord<String, String> record;

            if (partition != null) {
                // send messages to the specific partition
                record = KafkaProducerRecord.create(topic, null, "\"Hello-world - " + numSent.get() + "\"", partition);
            } else {
                record = KafkaProducerRecord.create(topic, "\"Hello-world - " + numSent.get() + "\"");
            }

            producer.send(record, done -> {
                if (done.succeeded()) {
                    RecordMetadata recordMetadata = done.result();
                    LOGGER.debug("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
                            ", partition=" + recordMetadata.getPartition() +
                            ", offset=" + recordMetadata.getOffset());

                    numSent.getAndIncrement();

                    if (msgCntPredicate.test(numSent.get())) {
                        LOGGER.info("Producer produced {} messages", numSent.get());
                        resultPromise.complete(numSent.get());
                    }

                    if (msgCntPredicate.negate().test(-1)) {
                        sendNext(producer, topic);
                    }

                } else {
                    LOGGER.warn("Producer cannot connect to topic {}: {}", topic, done.cause().toString());
                    sendNext(producer, topic);
                }
            });

        }
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }
}
