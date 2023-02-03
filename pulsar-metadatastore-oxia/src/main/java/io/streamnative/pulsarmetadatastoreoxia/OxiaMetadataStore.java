/*
 * Copyright © 2022-2023 StreamNative Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsarmetadatastoreoxia;


import io.streamnative.oxia.client.OxiaClientBuilder;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
import io.streamnative.oxia.client.api.Version;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;

@Slf4j
public class OxiaMetadataStore extends AbstractMetadataStore {

    private final AsyncOxiaClient client;

    OxiaMetadataStore(
            String serviceAddress, MetadataStoreConfig metadataStoreConfig, boolean enableSessionWatcher)
            throws Exception {
        super(metadataStoreConfig.getMetadataStoreName());
        var linger = metadataStoreConfig.getBatchingMaxDelayMillis();
        if (!metadataStoreConfig.isBatchingEnabled()) {
            linger = 0;
        }
        var builder =
                new OxiaClientBuilder(serviceAddress)
                        .notificationCallback(this::notificationCallback)
                        .batchLinger(Duration.ofMillis(linger))
                        .maxRequestsPerBatch(metadataStoreConfig.getBatchingMaxOperations());
        client = builder.asyncClient().get();
        super.registerSyncLister(Optional.ofNullable(metadataStoreConfig.getSynchronizer()));

        /* TODO use sessionTimeoutMillis

        TODO do we need to do anything with below configs?
        private final boolean allowReadOnlyOperations;  - seems optional
        private final String configFilePath;            - probably not
        private final int batchingMaxSizeKb;            - not supported by oxia
        private MetadataEventSynchronizer synchronizer; - is above en
        */

    }

    private void notificationCallback(Notification notification) {
        super.receivedNotification(convertNotification(notification));
    }

    private org.apache.pulsar.metadata.api.Notification convertNotification(
            Notification notification) {
        if (notification instanceof Notification.KeyCreated keyCreated) {
            return new org.apache.pulsar.metadata.api.Notification(
                    NotificationType.Created, keyCreated.key());
        }
        if (notification instanceof Notification.KeyModified keyModified) {
            return new org.apache.pulsar.metadata.api.Notification(
                    NotificationType.Modified, keyModified.key());
        }
        if (notification instanceof Notification.KeyDeleted keyDeleted) {
            return new org.apache.pulsar.metadata.api.Notification(
                    NotificationType.Deleted, keyDeleted.key());
        }
        log.error("Unknown notification type {}", notification);
        throw new RuntimeException("Unknown notification type");
    }

    Optional<GetResult> convertGetResult(
            String path, io.streamnative.oxia.client.api.GetResult result) {
        if (result == null) {
            return Optional.empty();
        }
        return Optional.of(result)
                .map(
                        oxiaResult ->
                                new GetResult(oxiaResult.getValue(), convertStat(path, oxiaResult.getVersion())));
    }

    Stat convertStat(String path, Version version) {
        return new Stat(
                path,
                version.versionId(),
                version.createdTimestamp(),
                version.modifiedTimestamp(),
                false, // TODO version.sessionId != null,
                false); // TODO version.createdBySelf());
    }

    @Override
    protected CompletableFuture<List<String>> getChildrenFromStore(String path) {
        var pathWithSlash = (path.endsWith("/")) ? path : (path + "/");

        return client
                .list(pathWithSlash, pathWithSlash + "/")
                .thenApply(
                        children ->
                                children.stream().map(child -> child.substring(pathWithSlash.length())).toList());
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        return client.get(path).thenApply(Objects::nonNull);
    }

    @Override
    protected CompletableFuture<Optional<GetResult>> storeGet(String path) {
        return client.get(path).thenApply(res -> convertGetResult(path, res));
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        return getChildrenFromStore(path)
                .thenCompose(
                        children -> {
                            if (children.size() > 0) {
                                return CompletableFuture.failedFuture(
                                        new MetadataStoreException("Key '" + path + "' has children"));
                            } else {
                                var delOption =
                                        expectedVersion
                                                .map(DeleteOption::ifVersionIdEquals)
                                                .orElse(DeleteOption.Unconditionally);
                                CompletableFuture<Boolean> result = client.delete(path, delOption);
                                return result
                                        .thenCompose(
                                                exists -> {
                                                    if (!exists) {
                                                        return CompletableFuture.failedFuture(
                                                                new MetadataStoreException.NotFoundException(
                                                                        "Key '" + path + "' does not exist"));
                                                    }
                                                    return CompletableFuture.completedFuture((Void) null);
                                                })
                                        .exceptionallyCompose(this::convertException);
                            }
                        });
    }

    @Override
    protected CompletableFuture<Stat> storePut(
            String path, byte[] data, Optional<Long> optExpectedVersion, EnumSet<CreateOption> options) {
        CompletableFuture<Void> parentsCreated = createParents(path);
        return parentsCreated.thenCompose(
                __ -> {
                    var expectedVersion = optExpectedVersion;
                    if (expectedVersion.isPresent()
                            && expectedVersion.get() != -1L
                            && options.contains(CreateOption.Sequential)) {
                        return CompletableFuture.failedFuture(
                                new MetadataStoreException(
                                        "Can't have expectedVersion and Sequential at the same time"));
                    }
                    CompletableFuture<String> actualPath;
                    if (options.contains(CreateOption.Sequential)) {
                        actualPath =
                                client
                                        .put(counterPath(path), new byte[] {})
                                        .thenApply(
                                                r -> String.format("%s%010d", path, r.version().modificationsCount()));
                        expectedVersion = Optional.of(-1L);
                    } else {
                        actualPath = CompletableFuture.completedFuture(path);
                    }
                    var ephemeral = options.contains(CreateOption.Ephemeral);
                    var versionCondition =
                            expectedVersion
                                    .map(
                                            ver -> {
                                                if (ver == -1) {
                                                    return PutOption.IfRecordDoesNotExist;
                                                }
                                                return PutOption.ifVersionIdEquals(ver);
                                            })
                                    .orElse(PutOption.Unconditionally);
                    var putOptions =
                            ephemeral
                                    ? new PutOption[] {PutOption.AsEphemeralRecord, versionCondition}
                                    : new PutOption[] {versionCondition};
                    return actualPath
                            .thenCompose(
                                    aPath ->
                                            client
                                                    .put(aPath, data, putOptions)
                                                    .thenApply(res -> new PathWithPutResult(aPath, res)))
                            .thenApply(res -> convertStat(res.path(), res.result().version()))
                            .exceptionallyCompose(this::convertException);
                });
    }

    private <T> CompletionStage<T> convertException(Throwable ex) {
        return (ex.getCause() instanceof UnexpectedVersionIdException)
                ? CompletableFuture.failedFuture(new MetadataStoreException.BadVersionException(ex.getCause()))
                : (ex.getCause() instanceof KeyAlreadyExistsException)
                        ? CompletableFuture.failedFuture(new MetadataStoreException.AlreadyExistsException(ex.getCause()))
                        : CompletableFuture.failedFuture(ex.getCause());
    }

    private CompletableFuture<Void> createParents(String path) {
        List<String> parentChain = getParentChain(path);
        return createParents(parentChain);
    }

    private CompletableFuture<Void> createParents(List<String> parentChain) {
        if (parentChain.size() == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return exists(parentChain.get(parentChain.size() - 1))
                .thenCompose(
                        exists -> {
                            if (exists) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                var immediate = parentChain.remove(parentChain.size() - 1);
                                return createImmediateParent(immediate).thenCompose(__ -> createParents(parentChain));
                            }
                        }).exceptionallyCompose(ex -> {
                            if (ex.getCause() instanceof KeyAlreadyExistsException) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return CompletableFuture.failedFuture(ex.getCause());
                });
    }

    private CompletableFuture<Void> createImmediateParent(String immediate) {
        return client.put(immediate, new byte[] {}, PutOption.IfRecordDoesNotExist)
                .thenApply(__ -> null);
    }

    private List<String> getParentChain(String path) {
        String[] split = path.split("/");
        var parentChain = new ArrayList<String>();
        var prefix = "";
        for (int i = 1; i < split.length - 1; ++i) {
            prefix = prefix + "/" + split[i];
            parentChain.add(prefix);
        }
        return parentChain;
    }

    private String counterPath(String path) {
        return "_oxia_pulsar/counter/" + path;
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            if (client != null) {
                client.close();
            }
            super.close();
        }
    }

    private record PathWithPutResult(String path, PutResult result) {}
}
