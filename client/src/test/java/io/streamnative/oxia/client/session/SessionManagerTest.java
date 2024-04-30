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
package io.streamnative.oxia.client.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Reassigned;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Removed;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionManagerTest {

    @Mock SessionFactory factory;
    @Mock Session session;
    SessionManager manager;

    @BeforeEach
    void setup() {
        manager = new SessionManager(factory);
    }

    @Test
    void newSession() {
        var shardId = 1L;
        when(factory.create(shardId)).thenReturn(session);
        assertThat(manager.getSession(shardId)).isSameAs(session);
        verify(factory).create(shardId);
        verify(session).start();
    }

    @Test
    void existingSession() {
        var shardId = 1L;
        when(factory.create(shardId)).thenReturn(session);
        var session1 = manager.getSession(shardId);
        verify(factory, times(1)).create(shardId);
        verify(session, times(1)).start();

        var session2 = manager.getSession(shardId);
        assertThat(session2).isSameAs(session1);
        verifyNoMoreInteractions(factory, session);
    }

    @Test
    void close() throws Exception {
        var shardId = 5L;
        when(session.getShardId()).thenReturn(shardId);
        when(factory.create(shardId)).thenReturn(session);
        doAnswer(
                        invocation -> {
                            manager.onSessionClosed(session);
                            return null;
                        })
                .when(session)
                .close();
        manager.getSession(shardId);

        assertThat(manager.sessions()).containsEntry(shardId, session);
        manager.close();
        await()
                .untilAsserted(
                        () -> {
                            verify(session).close();
                            assertThat(manager.sessions()).doesNotContainKey(shardId);
                        });
        assertThatThrownBy(() -> manager.getSession(shardId)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void accept() throws Exception {
        Session session21 = mock(Session.class);
        Session session22 = mock(Session.class);
        var shardId1 = 1L;
        var shardId2 = 2L;
        when(factory.create(shardId1)).thenReturn(session);
        when(factory.create(shardId2)).thenReturn(session21, session22);

        assertThat(manager.getSession(shardId1)).isSameAs(session);
        assertThat(manager.getSession(shardId2)).isSameAs(session21);

        manager.accept(
                new ShardAssignmentChanges(
                        Set.of(),
                        Set.of(new Removed(shardId1, "leader1")),
                        Set.of(new Reassigned(shardId2, "leader2", "leader3"))));

        assertThat(manager.sessions()).doesNotContainKey(shardId1);
        // Session here shouldn't have changed after the reassignment
        assertThat(manager.getSession(shardId2)).isSameAs(session21);
        verify(session).close();
    }

    @Test
    void testSessionClosed() throws Exception {
        var shardId = 1L;
        when(session.getShardId()).thenReturn(shardId);
        doAnswer(
                        invocation -> {
                            manager.onSessionClosed(session);
                            return null;
                        })
                .when(session)
                .close();
        when(factory.create(shardId)).thenReturn(session);
        manager.getSession(shardId);

        assertThat(manager.sessions()).containsEntry(shardId, session);

        session.close();
        assertThat(manager.sessions()).doesNotContainKey(shardId);
    }

    @Test
    void closeQuietly() throws Exception {
        var value = manager.closeQuietly(session);
        assertThat(value).containsSame(session);
        verify(session).close();
    }

    @Test
    void closeQuietlyNull() throws Exception {
        var value = manager.closeQuietly(null);
        assertThat(value).isEmpty();
    }
}
