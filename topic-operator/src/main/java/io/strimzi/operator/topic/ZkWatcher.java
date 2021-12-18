/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Base abstract class for a ZooKeeper watcher for child znodes
 */
public abstract class ZkWatcher {

    protected Logger log = LogManager.getLogger(getClass());

    protected final TopicOperator topicOperator;
    private volatile ZkWatcherState state = ZkWatcherState.NOT_STARTED;
    private volatile Zk zk;

    private final ConcurrentHashMap<String, Boolean> children = new ConcurrentHashMap<>();
    private final String rootZNode;

    enum ZkWatcherState {
        NOT_STARTED,    // = 0
        STARTED,       // = 1
        STOPPED         // = 2
    }

    ZkWatcher(TopicOperator topicOperator, String rootZNode) {
        this.topicOperator = topicOperator;
        this.rootZNode = rootZNode;
    }

    protected void start(Zk zk) {
        this.zk = zk;
        this.state = ZkWatcherState.STARTED;
    }

    protected void stop() {
        this.state = ZkWatcherState.STOPPED;
    }

    protected boolean started() {
        return this.state == ZkWatcherState.STARTED;
    }

    protected String getPath(String child) {
        return this.rootZNode + "/" + child;
    }

    /**
     * Add a child to watch under the root znode
     *
     * @param child child to watch
     */
    protected void addChild(String child) {
        this.children.put(child, false);
        String path = getPath(child);
        // Watching znode /config/topics/my-topic1 for changes
        // Watching znode /brokers/topics/my-topic1 for changes
        log.debug("Watching znode {} for changes", path);
        Handler<AsyncResult<byte[]>> handler = dataResult -> {
            if (dataResult.succeeded()) {
                this.children.compute(child, (k, v) -> {
                    if (v) {
                        this.notifyOperator(child);
                    }
                    return true;
                });
            } else {
                log.error("While getting or watching znode {}", path, dataResult.cause());
            }
        };
        zk.watchData(path, handler).compose(zk2 -> {
            zk.getData(path, handler);
            return Future.succeededFuture();
        });
    }

    protected void removeChild(String child) {
        log.debug("Unwatching znode {} for changes", child);
        this.children.remove(child);
        zk.unwatchData(getPath(child));
    }

    protected boolean watching(String child) {
        return this.children.containsKey(child);
    }

    protected abstract void notifyOperator(String child);
}
