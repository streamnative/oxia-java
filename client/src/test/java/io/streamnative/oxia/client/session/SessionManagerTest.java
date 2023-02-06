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
package io.streamnative.oxia.client.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionManagerTest {

    @Mock Session.Factory factory;
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
        var shardId = 1L;
        when(session.getSessionId()).thenReturn(shardId);
        when(factory.create(shardId)).thenReturn(session);
        manager.getSession(shardId);

        assertThat(manager.sessions()).containsEntry(shardId, session);
        manager.close();
        await()
                .untilAsserted(
                        () -> {
                            verify(session).close();
                            assertThat(manager.sessions()).doesNotContainKey(shardId);
                        });
    }
}
