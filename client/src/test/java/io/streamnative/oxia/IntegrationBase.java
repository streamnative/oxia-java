package io.streamnative.oxia;


import org.junit.AfterClass;
import org.junit.BeforeClass;

public class IntegrationBase {

    private static OxiaContainer container;

    @BeforeClass
    public static void setupOxia() {
        container = new OxiaContainer();
        container.start();
    }

    @AfterClass
    public static void tearDownOxia() {
        if (container != null) {
            container.stop();
        }
    }

    protected String getServiceAddress() {
        return container.getServiceAddress();
    }
}
