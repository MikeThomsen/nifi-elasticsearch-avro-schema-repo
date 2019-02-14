package org.apache.nifi

import org.apache.nifi.elasticsearch.ElasticSearchAvroSchemaRegistry
import org.apache.nifi.elasticsearch.ElasticSearchClientService
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl
import org.apache.nifi.serialization.record.StandardSchemaIdentifier
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test

class ElasticSearchAvroSchemaRegistryIT {
    TestRunner runner
    ElasticSearchAvroSchemaRegistry registry

    @Before
    void setup() {
        registry = new ElasticSearchAvroSchemaRegistry()
        def client = new ElasticSearchClientServiceImpl()
        runner = TestRunners.newTestRunner(MockProcessor.class)
        runner.addControllerService("registry", registry)
        runner.addControllerService("client", client)
        runner.setProperty(client, ElasticSearchClientService.HTTP_HOSTS, "http://localhost:9200")
        runner.setProperty(registry, ElasticSearchAvroSchemaRegistry.CLIENT_SERVICE, "client")
        runner.setProperty(MockProcessor.REGISTRY, "registry")
        runner.enableControllerService(client)
        runner.enableControllerService(registry)
        runner.assertValid()
    }

    @Test
    void test() {
        registry.retrieveSchema(new StandardSchemaIdentifier.Builder().name("test").build())
    }
}
