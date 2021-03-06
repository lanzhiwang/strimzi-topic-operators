/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

//
import io.strimzi.api.kafka.Crds;

//
import io.strimzi.certs.OpenSslCertManager;

//
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectS2IAssemblyOperator;
// topic user
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaBridgeAssemblyOperator;
// connector
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMaker2AssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator;

//
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;

//
import io.strimzi.operator.PlatformFeaturesAvailability;

//
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ClusterRoleOperator;

//
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;

//
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@SuppressFBWarnings("DM_EXIT")
public class Main {
    private static final Logger log = LogManager.getLogger(Main.class.getName());

    static {
        try {
            Crds.registerCustomKinds();
        } catch (Error | RuntimeException t) {
            t.printStackTrace();
        }
    }

    public static void main(String[] args) {
        log.info("ClusterOperator {} is starting", Main.class.getPackage().getImplementationVersion());
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(System.getenv());

        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        //Setup Micrometer metrics options
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)
        );
        Vertx vertx = Vertx.vertx(options);

        // Workaround for https://github.com/fabric8io/kubernetes-client/issues/2212
        // Can be removed after upgrade to Fabric8 4.10.2 or higher or to Java 11
        if (Util.shouldDisableHttp2()) {
            System.setProperty("http2.disable", "true");
        }

        KubernetesClient client = new DefaultKubernetesClient();

        maybeCreateClusterRoles(vertx, config, client).setHandler(crs -> {
            if (crs.succeeded()) {
                PlatformFeaturesAvailability.create(vertx, client).setHandler(pfa -> {
                    if (pfa.succeeded()) {
                        log.info("Environment facts gathered: {}", pfa.result());

                        run(vertx, client, pfa.result(), config).setHandler(ar -> {
                            if (ar.failed()) {
                                log.error("Unable to start operator for 1 or more namespace", ar.cause());
                                System.exit(1);
                            }
                        });
                    } else {
                        log.error("Failed to gather environment facts", pfa.cause());
                        System.exit(1);
                    }
                });
            } else  {
                log.error("Failed to create Cluster Roles", crs.cause());
                System.exit(1);
            }
        });
    }

    static CompositeFuture run(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, ClusterOperatorConfig config) {
        Util.printEnvInfo();

        OpenSslCertManager certManager = new OpenSslCertManager();
        PasswordGenerator passwordGenerator = new PasswordGenerator(
            12,
            "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "0123456789"
        );
        ResourceOperatorSupplier resourceOperatorSupplier = new ResourceOperatorSupplier(
            vertx,
            client,
            pfa,
            config.getOperationTimeoutMs()
        );

        //
        KafkaAssemblyOperator kafkaClusterOperations = new KafkaAssemblyOperator(
            vertx,
            pfa,
            certManager,
            passwordGenerator,
            resourceOperatorSupplier,
            config
        );

        //
        KafkaConnectAssemblyOperator kafkaConnectClusterOperations = new KafkaConnectAssemblyOperator(
            vertx,
            pfa,
            resourceOperatorSupplier,
            config
        );

        //
        KafkaConnectS2IAssemblyOperator kafkaConnectS2IClusterOperations = null;
        if (pfa.supportsS2I()) {
            kafkaConnectS2IClusterOperations = new KafkaConnectS2IAssemblyOperator(vertx, pfa, resourceOperatorSupplier, config);
        } else {
            log.info("The KafkaConnectS2I custom resource definition can only be used in environment which supports OpenShift build, image and apps APIs. These APIs do not seem to be supported in this environment.");
        }

        //
        KafkaMirrorMaker2AssemblyOperator kafkaMirrorMaker2AssemblyOperator = new KafkaMirrorMaker2AssemblyOperator(
            vertx,
            pfa,
            resourceOperatorSupplier,
            config
        );

        //
        KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator = new KafkaMirrorMakerAssemblyOperator(
            vertx,
            pfa,
            certManager,
            passwordGenerator,
            resourceOperatorSupplier,
            config
        );

        //
        KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator = new KafkaBridgeAssemblyOperator(
            vertx,
            pfa,
            certManager,
            passwordGenerator,
            resourceOperatorSupplier,
            config
        );

        KafkaRebalanceAssemblyOperator kafkaRebalanceAssemblyOperator = new KafkaRebalanceAssemblyOperator(
            vertx,
            pfa,
            resourceOperatorSupplier
        );

        List<Future> futures = new ArrayList<>(config.getNamespaces().size());
        for (String namespace : config.getNamespaces()) {
            Promise<String> prom = Promise.promise();
            futures.add(prom.future());
            ClusterOperator operator = new ClusterOperator(
                namespace,
                config.getReconciliationIntervalMs(),
                client,
                kafkaClusterOperations,
                kafkaConnectClusterOperations,
                kafkaConnectS2IClusterOperations,
                kafkaMirrorMakerAssemblyOperator,
                kafkaMirrorMaker2AssemblyOperator,
                kafkaBridgeAssemblyOperator,
                kafkaRebalanceAssemblyOperator,
                resourceOperatorSupplier.metricsProvider
            );
            vertx.deployVerticle(operator, res -> {
                if (res.succeeded()) {
                    log.info("Cluster Operator verticle started in namespace {}", namespace);
                } else {
                    log.error("Cluster Operator verticle in namespace {} failed to start", namespace, res.cause());
                    System.exit(1);
                }
                prom.handle(res);
            });
        }
        return CompositeFuture.join(futures);
    }

    /*test*/ static Future<Void> maybeCreateClusterRoles(Vertx vertx, ClusterOperatorConfig config, KubernetesClient client) {
        if (config.isCreateClusterRoles()) {
            List<Future> futures = new ArrayList<>();
            ClusterRoleOperator cro = new ClusterRoleOperator(vertx, client, config.getOperationTimeoutMs());

            Map<String, String> clusterRoles = new HashMap<String, String>() {
                {
                    put("strimzi-cluster-operator-namespaced", "020-ClusterRole-strimzi-cluster-operator-role.yaml");
                    put("strimzi-cluster-operator-global", "021-ClusterRole-strimzi-cluster-operator-role.yaml");
                    put("strimzi-kafka-broker", "030-ClusterRole-strimzi-kafka-broker.yaml");
                    put("strimzi-entity-operator", "031-ClusterRole-strimzi-entity-operator.yaml");
                    put("strimzi-topic-operator", "032-ClusterRole-strimzi-topic-operator.yaml");
                }
            };

            for (Map.Entry<String, String> clusterRole : clusterRoles.entrySet()) {
                log.info("Creating cluster role {}", clusterRole.getKey());

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(Main.class.getResourceAsStream("/cluster-roles/" + clusterRole.getValue()),
                                StandardCharsets.UTF_8))) {
                    String yaml = br.lines().collect(Collectors.joining(System.lineSeparator()));
                    ClusterRole role = cro.convertYamlToClusterRole(yaml);
                    Future fut = cro.reconcile(role.getMetadata().getName(), role);
                    futures.add(fut);
                } catch (IOException e) {
                    log.error("Failed to create Cluster Roles.", e);
                    throw new RuntimeException(e);
                }

            }

            Promise<Void> returnPromise = Promise.promise();
            CompositeFuture.all(futures).setHandler(res -> {
                if (res.succeeded())    {
                    returnPromise.complete();
                } else  {
                    returnPromise.fail("Failed to create Cluster Roles.");
                }
            });

            return returnPromise.future();
        } else {
            return Future.succeededFuture();
        }
    }
}
