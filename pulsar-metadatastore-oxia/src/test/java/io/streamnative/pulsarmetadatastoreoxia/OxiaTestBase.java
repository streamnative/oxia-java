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


import io.streamnative.oxia.testcontainers.OxiaContainer;
import java.util.function.Supplier;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public interface OxiaTestBase {

    OxiaContainer getContainer();

    void setContainer(OxiaContainer container);

    @BeforeClass
    default void setupOxia() {
        System.setProperty(
                MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY,
                OxiaMetadataStoreProvider.class.getName());
        setContainer(new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME));
        getContainer().start();
    }

    @AfterClass
    default void teardownOxia() {
        if (getContainer() != null) {
            getContainer().stop();
        }
    }

    default Object[][] impl() {
        return new Object[][] {
            {
                "Oxia",
                (Supplier<String>)
                        (() -> "oxia://localhost:" + getContainer().getMappedPort(OxiaContainer.OXIA_PORT))
            }
        };
    }
}
