/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class CruiseControlUtils {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlUtils.class);

    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2

    private CruiseControlUtils() { }

    @SuppressWarnings("BooleanExpressionComplexity")
    public static void verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(Properties kafkaProperties) {
        TestUtils.waitFor("Verify that kafka configuration " + kafkaProperties.toString() + " has correct cruise control metric reporter properties",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_CRUISE_CONTROL_TIMEOUT, () ->
            kafkaProperties.getProperty("cruise.control.metrics.topic").equals("strimzi.cruisecontrol.metrics") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.ssl.endpoint.identification.algorithm").equals("HTTPS") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.bootstrap.servers").equals("my-cluster-kafka-bootstrap:9091") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.security.protocol").equals("SSL") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.ssl.keystore.type").equals("PKCS12") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.ssl.keystore.location").equals("/tmp/kafka/cluster.keystore.p12") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.ssl.keystore.password").equals("${CERTS_STORE_PASSWORD}") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.ssl.truststore.type").equals("PKCS12") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.ssl.truststore.location").equals("/tmp/kafka/cluster.truststore.p12") &&
            kafkaProperties.getProperty("cruise.control.metrics.reporter.ssl.truststore.password").equals("${CERTS_STORE_PASSWORD}"));
    }

    public static void verifyThatCruiseControlTopicsArePresent() {
        final int numberOfPartitionsMetricTopic = 1;
        final int numberOfPartitionsSamplesTopic = 32;
        final int numberOfReplicasMetricTopic = 1;
        final int numberOfReplicasSamplesTopic = 2;

        KafkaTopic metrics = KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(CRUISE_CONTROL_METRICS_TOPIC).get();
        KafkaTopic modelTrainingSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get();
        KafkaTopic partitionsMetricsSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get();

        TestUtils.waitFor("Verify that kafka contains cruise control topics with related configuration.",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_CRUISE_CONTROL_TIMEOUT, () -> {

                boolean hasTopicCorrectPartitionsCount =
                    metrics.getSpec().getPartitions() == numberOfPartitionsMetricTopic &&
                    modelTrainingSamples.getSpec().getPartitions() == numberOfPartitionsSamplesTopic &&
                    partitionsMetricsSamples.getSpec().getPartitions() == numberOfPartitionsSamplesTopic;

                boolean hasTopicCorrectReplicasCount =
                    metrics.getSpec().getReplicas() == numberOfReplicasMetricTopic &&
                    modelTrainingSamples.getSpec().getReplicas() == numberOfReplicasSamplesTopic &&
                    partitionsMetricsSamples.getSpec().getReplicas() == numberOfReplicasSamplesTopic;

                return hasTopicCorrectPartitionsCount && hasTopicCorrectReplicasCount;
            });
    }

    public static Properties getKafkaCruiseControlMetricsReporterConfiguration(String clusterName) throws IOException {
        InputStream configurationFileStream = new ByteArrayInputStream(kubeClient().getConfigMap(
            KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName)).getData().get("server.config").getBytes(StandardCharsets.UTF_8));

        Properties configurationOfKafka = new Properties();
        configurationOfKafka.load(configurationFileStream);
        LOGGER.info("Verifying that in {} is not present configuration related to metrics reporter", KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName));

        Properties cruiseControlProperties = new Properties();

        for (Map.Entry<Object, Object> entry : configurationOfKafka.entrySet()) {
            if (entry.getKey().toString().startsWith("cruise.control.metrics")) {
                cruiseControlProperties.put(entry.getKey(), entry.getValue());
            }
        }

        return cruiseControlProperties;
    }
}
