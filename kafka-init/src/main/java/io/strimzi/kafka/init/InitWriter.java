/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.client.KubernetesClient;

import io.strimzi.operator.cluster.model.NodeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InitWriter {

    private static final Logger log = LogManager.getLogger(InitWriter.class);

    private KubernetesClient client;
    private InitWriterConfig config;

    protected final static String FILE_RACK_ID = "rack.id";
    protected final static String FILE_EXTERNAL_ADDRESS = "external.address";

    // InitWriter writer = new InitWriter(client, config);
    public InitWriter(KubernetesClient client, InitWriterConfig config) {
        this.client = client;
        this.config = config;
    }

    /**
     * Write the rack-id
     *
     * @return if the operation was executed successfully
     */
    public boolean writeRack() {

        Map<String, String> nodeLabels = client.nodes().withName(config.getNodeName()).get().getMetadata().getLabels();
        log.info("NodeLabels = {}", nodeLabels);
        String rackId = nodeLabels.get(config.getRackTopologyKey());
        log.info("Rack: {} = {}", config.getRackTopologyKey(), rackId);

        if (rackId == null) {
            log.error("Node {} doesn't have the label {} for getting the rackid",
                    config.getNodeName(), config.getRackTopologyKey());
            return false;
        }

        return write(FILE_RACK_ID, rackId);
    }

    /**
     * Write the external address of this node
     *
     * @return if the operation was executed successfully
     */
    public boolean writeExternalAddress() {

        List<NodeAddress> addresses = client.nodes().withName(config.getNodeName()).get().getStatus().getAddresses();
        log.info("NodeLabels = {}", addresses);
        String externalAddress = NodeUtils.findAddress(addresses, config.getAddressType());

        if (externalAddress == null) {
            log.error("External address not found");
            return false;
        } else  {
            log.info("External address found {}", externalAddress);
        }

        return write(FILE_EXTERNAL_ADDRESS, externalAddress);
    }

    /**
    $ pwd
    /opt/kafka/init

    $ ll
    total 4
    drwxrwxrwx 2 root  root  30 Nov 12 15:22 ./
    drwxr-xr-x 1 root  root 106 Nov 12 15:22 ../
    -rw-r--r-- 1 kafka root  15 Nov 12 15:22 external.address

    $ cat external.address
    192.168.132.209

    $ kubectl -n hz-kafka get pods -o wide
    NAME                                                READY   STATUS    RESTARTS   AGE   IP           NODE              NOMINATED NODE   READINESS GATES
    my-cluster-kafka-entity-operator-66f6ddd84c-s75hc   3/3     Running   0          22h   10.3.1.239   192.168.132.209   <none>           <none>
    my-cluster-kafka-kafka-0                            2/2     Running   0          22h   10.3.1.234   192.168.132.209   <none>           <none>
    my-cluster-kafka-kafka-1                            2/2     Running   2          22h   10.3.1.235   192.168.132.209   <none>           <none>
    my-cluster-kafka-kafka-2                            2/2     Running   0          22h   10.3.1.236   192.168.132.209   <none>           <none>
    my-cluster-kafka-zoo-entrance-6c8955b584-hgnh8      1/1     Running   0          22h   10.3.1.240   192.168.132.208   <none>           <none>
    my-cluster-kafka-zookeeper-0                        1/1     Running   0          22h   10.3.1.231   192.168.132.209   <none>           <none>
    my-cluster-kafka-zookeeper-1                        1/1     Running   0          22h   10.3.1.232   192.168.132.209   <none>           <none>
    my-cluster-kafka-zookeeper-2                        1/1     Running   0          22h   10.3.1.233   192.168.132.209   <none>           <none>
    */
    // write(FILE_RACK_ID, rackId);
    // write(FILE_EXTERNAL_ADDRESS, externalAddress);
    private boolean write(String file, String information) {
        boolean isWritten;

        try (PrintWriter writer = new PrintWriter(config.getInitFolder() + "/" + file, "UTF-8")) {
            writer.write(information);

            if (writer.checkError()) {
                log.error("Failed to write the information {} to file {}", information, file);
                isWritten = false;
            } else {
                log.info("Information {} written successfully to file {}", information, file);
                isWritten = true;
            }
        } catch (IOException e) {
            log.error("Error writing the information {} to file {}", information, file, e);
            isWritten = false;
        }

        return isWritten;
    }

    /**
     * Tries to find the right address of the node. The different addresses has different prioprities:
     *      1. ExternalDNS
     *      2. ExternalIP
     *      3. Hostname
     *      4. InternalDNS
     *      5. InternalIP
     *
     * @param addresses List of addresses which are assigned to our node
     * @return  Address of the node
     */
    protected String findAddress(List<NodeAddress> addresses)   {
        if (addresses == null)  {
            return null;
        }

        Map<String, String> addressMap = addresses.stream().collect(Collectors.toMap(NodeAddress::getType, NodeAddress::getAddress));

        // If user set preferred address type, we should check it first
        if (config.getAddressType() != null && addressMap.containsKey(config.getAddressType())) {
            return addressMap.get(config.getAddressType());
        }

        if (addressMap.containsKey("ExternalDNS"))  {
            return addressMap.get("ExternalDNS");
        } else if (addressMap.containsKey("ExternalIP"))  {
            return addressMap.get("ExternalIP");
        } else if (addressMap.containsKey("InternalDNS"))  {
            return addressMap.get("InternalDNS");
        } else if (addressMap.containsKey("InternalIP"))  {
            return addressMap.get("InternalIP");
        } else if (addressMap.containsKey("Hostname")) {
            return addressMap.get("Hostname");
        }

        return null;
    }
}
