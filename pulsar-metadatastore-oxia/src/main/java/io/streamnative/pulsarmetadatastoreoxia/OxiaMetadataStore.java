/*
 * Copyright © 2022-2024 StreamNative Inc.
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

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.Version;
import io.streamnative.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;

@Slf4j
public class OxiaMetadataStore extends AbstractMetadataStore {

    private final AsyncOxiaClient client;

    private final String identity;
    private final Optional<MetadataEventSynchronizer> synchronizer;

    OxiaMetadataStore(
            @NonNull String serviceAddress,
            @NonNull String namespace,
            @NonNull MetadataStoreConfig metadataStoreConfig,
            boolean enableSessionWatcher)
            throws Exception {
        super("oxia-metadata");

        var linger = metadataStoreConfig.getBatchingMaxDelayMillis();
        if (!metadataStoreConfig.isBatchingEnabled()) {
            linger = 0;
        }
        this.synchronizer = Optional.ofNullable(metadataStoreConfig.getSynchronizer());
        identity = UUID.randomUUID().toString();
        client =
                OxiaClientBuilder.create(serviceAddress)
                        .clientIdentifier(identity)
                        .namespace(namespace)
                        .sessionTimeout(Duration.ofMillis(metadataStoreConfig.getSessionTimeoutMillis()))
                        .batchLinger(Duration.ofMillis(linger))
                        .maxRequestsPerBatch(metadataStoreConfig.getBatchingMaxOperations())
                        .asyncClient()
                        .get();
        client.notifications(this::notificationCallback);
        super.registerSyncLister(Optional.ofNullable(metadataStoreConfig.getSynchronizer()));
    }

    private void notificationCallback(Notification notification) {
        if (notification.key().startsWith("__oxia")) {
            return;
        }
        if (notification instanceof Notification.KeyCreated keyCreated) {
            receivedNotification(
                    new org.apache.pulsar.metadata.api.Notification(
                            NotificationType.Created, keyCreated.key()));
            notifyParentChildrenChanged(keyCreated.key());

        } else if (notification instanceof Notification.KeyModified keyModified) {
            receivedNotification(
                    new org.apache.pulsar.metadata.api.Notification(
                            NotificationType.Modified, keyModified.key()));
        } else if (notification instanceof Notification.KeyDeleted keyDeleted) {
            receivedNotification(
                    new org.apache.pulsar.metadata.api.Notification(
                            NotificationType.Deleted, keyDeleted.key()));
            notifyParentChildrenChanged(keyDeleted.key());
        } else {
            log.error("Unknown notification type {}", notification);
        }
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
                version.sessionId().isPresent(),
                version.clientIdentifier().stream().anyMatch(identity::equals),
                version.modificationsCount() == 0);
    }

    @Override
    protected CompletableFuture<List<String>> getChildrenFromStore(String path) {
        var pathWithSlash = path + "/";

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
                            if (!children.isEmpty()) {
                                return CompletableFuture.failedFuture(
                                        new MetadataStoreException("Key '" + path + "' has children"));
                            } else {
                                Set<DeleteOption> deleteOptions =
                                        expectedVersion
                                                .map(v -> Set.of(DeleteOption.IfVersionIdEquals(v)))
                                                .orElse(Collections.emptySet());

                                return client
                                        .delete(path, deleteOptions)
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
                        var parent = parent(path);
                        var parentPath = parent == null ? "/" : parent;

                        actualPath =
                                client
                                        .put(parentPath, new byte[] {})
                                        .thenApply(
                                                r -> String.format("%s%010d", path, r.version().modificationsCount()));
                        expectedVersion = Optional.of(-1L);
                    } else {
                        actualPath = CompletableFuture.completedFuture(path);
                    }
                    Set<PutOption> putOptions = new HashSet<>();
                    expectedVersion
                            .map(
                                    ver -> {
                                        if (ver == -1) {
                                            return PutOption.IfRecordDoesNotExist;
                                        }
                                        return PutOption.IfVersionIdEquals(ver);
                                    })
                            .ifPresent(putOptions::add);

                    if (options.contains(CreateOption.Ephemeral)) {
                        putOptions.add(PutOption.AsEphemeralRecord);
                    }
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
        return (ex.getCause() instanceof UnexpectedVersionIdException
                        || ex.getCause() instanceof KeyAlreadyExistsException)
                ? CompletableFuture.failedFuture(
                        new MetadataStoreException.BadVersionException(ex.getCause()))
                : CompletableFuture.failedFuture(ex.getCause());
    }

    private CompletableFuture<Void> createParents(String path) {
        var parent = parent(path);
        if (parent == null || parent.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return exists(parent)
                .thenCompose(
                        exists -> {
                            if (exists) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                return client
                                        .put(
                                                parent,
                                                new byte[] {},
                                                Collections.singleton(PutOption.IfRecordDoesNotExist))
                                        .thenCompose(__ -> createParents(parent));
                            }
                        })
                .exceptionallyCompose(
                        ex -> {
                            if (ex.getCause() instanceof KeyAlreadyExistsException) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return CompletableFuture.failedFuture(ex.getCause());
                        });
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

    public Optional<MetadataEventSynchronizer> getMetadataEventSynchronizer() {
        return synchronizer;
    }

    private record PathWithPutResult(String path, PutResult result) {}
}
