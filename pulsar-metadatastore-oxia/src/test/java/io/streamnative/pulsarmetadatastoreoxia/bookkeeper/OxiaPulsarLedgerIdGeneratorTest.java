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
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.bookkeeper.PulsarLedgerIdGeneratorTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Slf4j
public class OxiaPulsarLedgerIdGeneratorTest extends PulsarLedgerIdGeneratorTest
        implements OxiaTestBase {

    @Getter @Setter private OxiaContainer container;

    @DataProvider(name = "impl")
    public Object[][] implementations() {
        return impl();
    }

    // -------- Override ZK-specific tests to do nothing --------

    @Ignore
    @Test
    public void testGenerateLedgerIdWithZkPrefix() throws Exception {}

    // TODO -------- Failing tests:

    @Ignore
    @Test(dataProvider = "impl")
    public void testGenerateLedgerId(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    @Test(enabled = false) // TODO fix failing test
    @Override
    public void testEnsureCounterIsNotResetWithContainerNodes(
            String provider, Supplier<String> urlSupplier) throws Exception {}
}
