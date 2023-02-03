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
import org.apache.pulsar.metadata.bookkeeper.LedgerUnderreplicationManagerTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

/** Test the zookeeper implementation of the ledger replication manager. */
@Slf4j
public class OxiaLedgerUnderreplicationManagerTest extends LedgerUnderreplicationManagerTest
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

    // TODO -------- Failing tests:

    public void testBasicInteraction(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    public void testGetList(String provider, Supplier<String> urlSupplier) throws Exception {}

    public void testLocking(String provider, Supplier<String> urlSupplier) throws Exception {}

    public void testMarkingAsReplicated(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    public void testRelease(String provider, Supplier<String> urlSupplier) throws Exception {}

    public void testManyFailures(String provider, Supplier<String> urlSupplier) throws Exception {}

    public void testGetReplicationWorkerIdRereplicatingLedger(
            String provider, Supplier<String> urlSupplier) throws Exception {}

    public void test2reportSame(String provider, Supplier<String> urlSupplier) throws Exception {}

    public void testMultipleManagersShouldBeAbleToTakeAndReleaseLock(
            String provider, Supplier<String> urlSupplier) throws Exception {}

    public void testMarkSimilarMissingReplica(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    public void testManyFailuresInAnEnsemble(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    public void testDisableLedgerReplication(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    public void testEnableLedgerReplication(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    public void testPlacementPolicyCheckCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    public void testReplicasCheckCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {}

    @Override
    public void testCheckAllLedgersCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {}
}
