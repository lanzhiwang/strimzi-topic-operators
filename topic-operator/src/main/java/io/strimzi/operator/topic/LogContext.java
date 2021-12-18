/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.KafkaTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class LogContext {

    private final static Logger LOGGER = LogManager.getLogger(K8sTopicWatcher.class);

    private static AtomicInteger ctx = new AtomicInteger();
    private final String base;
    private final String trigger;
    private String resourceVersion;

    private LogContext(String trigger) {
        base = ctx.getAndIncrement() + "|" + trigger;
        this.trigger = trigger;
    }

    // LogContext logContext = LogContext.zkWatch(/brokers/topics-my-topic1);  直接从 kafka 删除 topic
    // LogContext logContext = LogContext.zkWatch(/brokers/topics+my-topic1);  直接从 kafka 添加 topic
    // LogContext logContext = LogContext.zkWatch(/brokers/topics=my-topic1);  从 kafka 修改分区
    // LogContext logContext = LogContext.zkWatch(/config/topics=my-topic1);   从 kafka 修改 topic 配置
    static LogContext zkWatch(String znode, String childAction) {
        return new LogContext(znode + " " + childAction);
    }

    // LogContext logContext = LogContext.kubeWatch(action, kafkaTopic).withKubeTopic(kafkaTopic);
    static LogContext kubeWatch(Watcher.Action action, KafkaTopic kafkaTopic) {
        // kube +my-topic  k8s add
        // kube -my-topic  k8s delete
        // kube =my-topic  k8s modify
        // kube !my-topic
        LogContext logContext = new LogContext("kube " + action(action) + kafkaTopic.getMetadata().getName());
        // 8551873
        logContext.resourceVersion = kafkaTopic.getMetadata().getResourceVersion();
        return logContext;
    }

    // LogContext logContext = LogContext.periodic(reconciliationType + "kube " + kt.getMetadata().getName()).withKubeTopic(kt);
    // LogContext logContext = LogContext.periodic(reconciliationType + "-" + tn);
    // LogContext logContext = LogContext.periodic(reconciliationType + "kafka " + topicName);
    static LogContext periodic(String periodicType) {
        return new LogContext(periodicType);
    }

    private static String action(Watcher.Action action) {
        switch (action) {
            case ADDED:
                return "+";
            case MODIFIED:
                return "=";
            case DELETED:
                return "-";
        }
        return "!";
    }

    public String trigger() {
        return trigger;
    }

    @Override
    public String toString() {
        if (resourceVersion == null) {
            return base;
        } else {
            return base + "|" + resourceVersion;
        }
    }

    // LogContext logContext = LogContext.kubeWatch(action, kafkaTopic).withKubeTopic(kafkaTopic);
    public LogContext withKubeTopic(KafkaTopic kafkaTopic) {
        String newResourceVersion = kafkaTopic == null ? null : kafkaTopic.getMetadata().getResourceVersion();
        if (!Objects.equals(resourceVersion, newResourceVersion)) {
            LOGGER.debug("{}: Concurrent modification in kube: new version {}", this, newResourceVersion);
        }
        this.resourceVersion = newResourceVersion;
        return this;
    }
}
