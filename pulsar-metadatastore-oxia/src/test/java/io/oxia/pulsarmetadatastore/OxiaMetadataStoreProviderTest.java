/*
 * Copyright © 2022-2025 StreamNative Inc.
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
package io.oxia.pulsarmetadatastore;

import io.oxia.testcontainers.OxiaContainer;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.junit.Assert;
import org.testng.annotations.Test;

public class OxiaMetadataStoreProviderTest implements OxiaTestBase {

    @Getter @Setter private OxiaContainer container;

    @Test
    public void testCreateWithoutNamespace() throws Exception {
        var store =
                MetadataStoreFactory.create(
                        "oxia://localhost:" + getContainer().getMappedPort(OxiaContainer.OXIA_PORT),
                        MetadataStoreConfig.builder().build());
        store.close();
    }

    @Test
    public void testCreateWithDefaultNamespace() throws Exception {
        var store =
                MetadataStoreFactory.create(
                        "oxia://localhost:"
                                + getContainer().getMappedPort(OxiaContainer.OXIA_PORT)
                                + "/default",
                        MetadataStoreConfig.builder().build());
        store.close();
    }

    @Test
    public void testCreateWithIllegalURL() {
        // Test with multiple url parts
        try {
            var store =
                    MetadataStoreFactory.create(
                            "oxia://localhost:"
                                    + getContainer().getMappedPort(OxiaContainer.OXIA_PORT)
                                    + "/default/aaa",
                            MetadataStoreConfig.builder().build());
            Assert.fail("Expect failed by illegal url");
        } catch (MetadataStoreException ex) {

        }
    }
}
