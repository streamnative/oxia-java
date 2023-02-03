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


import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;

public class OxiaMetadataStoreProvider implements MetadataStoreProvider {

    @Override
    public String urlScheme() {
        return "oxia";
    }

    @Override
    public MetadataStore create(
            String metadataURL, MetadataStoreConfig metadataStoreConfig, boolean enableSessionWatcher)
            throws MetadataStoreException {
        var serviceAddress = getServiceAddress(metadataURL);
        try {
            return new OxiaMetadataStore(serviceAddress, metadataStoreConfig, enableSessionWatcher);
        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    String getServiceAddress(String metadataURL) throws MetadataStoreException {
        if (!metadataURL.startsWith(urlScheme() + "://")) {
            throw new MetadataStoreException("Invalid metadata URL. Must start with 'oxia://'.");
        }
        return metadataURL.substring("oxia://".length());
    }
}
