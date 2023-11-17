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
package io.streamnative.pulsarmetadatastoreoxia.bookkeeper;

import io.streamnative.oxia.testcontainers.OxiaContainer;
import io.streamnative.pulsarmetadatastoreoxia.OxiaTestBase;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.bookkeeper.PulsarRegistrationClientTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

@Slf4j
public class OxiaPulsarRegistrationClientTest extends PulsarRegistrationClientTest
        implements OxiaTestBase {

    @Getter @Setter private OxiaContainer container;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        this.incrementSetupNumber();
    }

    @DataProvider(name = "impl")
    public Object[][] implementations() {
        return impl();
    }
}
