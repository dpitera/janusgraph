package org.janusgraph.graphdb;

import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.ConfigurationGraphManagement;
import org.janusgraph.graphdb.management.utils.ConfigurationGraphManagementNotEnabled;
import org.janusgraph.core.JanusConfiguredGraphFactory;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.management.ConfigurationGraphManagement.*;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.HashMap;
import org.junit.Test;
import static org.junit.Assert.*;

public class JanusConfiguredGraphFactoryTest {
    private static JanusGraphManager gm;
    private static ConfigurationGraphManagement configGraphManagement;
    static {
        gm = new JanusGraphManager(new Settings());
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        MapConfiguration config = new MapConfiguration(map);
        StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(config)));
        configGraphManagement = new ConfigurationGraphManagement(graph);
    }

    @Test
    public void shouldGetConfigurationGraphManagementInstance() throws ConfigurationGraphManagementNotEnabled {
        ConfigurationGraphManagement thisInstance = ConfigurationGraphManagement.getInstance();
        assertNotNull(thisInstance);
    }

    @Test
    public void shouldOpenGraphUsingConfiguration() throws InterruptedException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        StandardJanusGraph graph = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertNotNull(graph);

        configGraphManagement.removeConfiguration("graph1");
        JanusGraphFactory.close("graph1");
    }
    
    @Test
    public void shouldCreateAndGetGraphUsingTemplateConfiguration() throws InterruptedException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        configGraphManagement.createTemplateConfiguration(new MapConfiguration(map)); 
        StandardJanusGraph graph = (StandardJanusGraph) JanusConfiguredGraphFactory.create("graph1");
        StandardJanusGraph graph1 = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");

        assertNotNull(graph);
        assertEquals(graph, graph1);

        configGraphManagement.removeTemplateConfiguration();
        JanusGraphFactory.close("graph1");
    }

    @Test
    public void shouldThrowConfigurationDoesNotExistError() {
        boolean errorThrown = false;
        try {
            JanusConfiguredGraphFactory.open("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }
        assertTrue(errorThrown);
    }
    
    @Test
    public void shouldThrowTemplateConfigurationDoesNotExistError() {
        boolean errorThrown = false;
        try {
            JanusConfiguredGraphFactory.create("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }
        assertTrue(errorThrown);
    }

    @Test
    public void storageDirectoryShouldBeStorageRootPlusGraphName() throws InterruptedException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "hbase");
        map.put(STORAGE_ROOT.toStringWithoutRoot(), "./tmp");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        
        StandardJanusGraph graph = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertNotNull(graph);
        assertEquals(graph.getConfiguration().getConfiguration().get(STORAGE_DIRECTORY), "./tmp/graph1");
        
        configGraphManagement.removeConfiguration("graph1");
        JanusGraphFactory.close("graph1");
    }

    @Test
    public void shouldThrowErrorNeedToSupplyGraphNameAndStorageRoot() {
        boolean errorThrown = false;

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "hbase");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        
        try {
            JanusConfiguredGraphFactory.open("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }
        assertTrue(errorThrown);
        
        configGraphManagement.removeConfiguration("graph1");
    }

    @Test
    public void cassandraKeyspaceShouldEqualGraphName() throws InterruptedException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "cassandra");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        
        StandardJanusGraph graph = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertNotNull(graph);
        assertEquals(graph.getConfiguration().getConfiguration().get(CASSANDRA_KEYSPACE), "graph1");
        
        configGraphManagement.removeConfiguration("graph1");
        JanusGraphFactory.close("graph1");
    }

    @Test
    public void cassandraKeyspaceShouldEqualSuppliedName() throws InterruptedException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "cassandra");
        map.put(CASSANDRA_KEYSPACE.toStringWithoutRoot(), "randomKeyspace");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        
        StandardJanusGraph graph = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertNotNull(graph);
        assertEquals(graph.getConfiguration().getConfiguration().get(CASSANDRA_KEYSPACE), "randomKeyspace");
        
        configGraphManagement.removeConfiguration("graph1");
        JanusGraphFactory.close("graph1");
    }

    @Test
    public void shouldGetGraphAfterRemoveConfigurationBecauseOnceOpenedTheReferenceIsStoredInGraphManager()
        throws InterruptedException {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        StandardJanusGraph graph = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertNotNull(graph);

        configGraphManagement.removeConfiguration("graph1");
        
        StandardJanusGraph graph1 = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        
        assertEquals(graph, graph1);

        JanusGraphFactory.close("graph1");
    }

    @Test
    public void shouldFailToOpenNewGraphAfterRemoveConfiguration() {
        boolean errorThrown = false;

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        configGraphManagement.removeConfiguration("graph1");
        try {
            JanusConfiguredGraphFactory.open("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }

        assertTrue(errorThrown);
    }

    @Test
    public void shouldFailToCreateGraphAfterRemoveTemplateConfiguration() {
        boolean errorThrown = false;
        
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        configGraphManagement.createTemplateConfiguration(new MapConfiguration(map)); 
        configGraphManagement.removeTemplateConfiguration();
        
        try {
            JanusConfiguredGraphFactory.create("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }
        
        assertTrue(errorThrown);
    }

    @Test
    public void updateConfigurationShouldOnlyUpdateForGraphAfterWeCloseAndReOpen() throws InterruptedException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "cassandra");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        map.put(CASSANDRA_KEYSPACE.toStringWithoutRoot(), "graph1Keyspace");
        configGraphManagement.createConfiguration(new MapConfiguration(map)); 
        StandardJanusGraph graph = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertNotNull(graph);
        assertEquals(graph.getConfiguration().getConfiguration().get(CASSANDRA_KEYSPACE), "graph1Keyspace");

        map.put(CASSANDRA_KEYSPACE.toStringWithoutRoot(), "graph1KeyspaceUpdated");
        configGraphManagement.updateConfiguration("graph1", new MapConfiguration(map));
        
        StandardJanusGraph graph1 = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertEquals(graph1.getConfiguration().getConfiguration().get(CASSANDRA_KEYSPACE), "graph1Keyspace");

        JanusGraphFactory.close("graph1");
        
        StandardJanusGraph graph2 = (StandardJanusGraph) JanusConfiguredGraphFactory.open("graph1");
        assertNotNull(graph2);
        assertEquals(graph2.getConfiguration().getConfiguration().get(CASSANDRA_KEYSPACE), "graph1KeyspaceUpdated");

        configGraphManagement.removeConfiguration("graph1");
        JanusGraphFactory.close("graph1");
    }

    @Test
    public void shouldCreateTwoGraphsUsingSameTemplateConfiguration() throws InterruptedException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        configGraphManagement.createTemplateConfiguration(new MapConfiguration(map)); 
        StandardJanusGraph graph1 = (StandardJanusGraph) JanusConfiguredGraphFactory.create("graph1");
        StandardJanusGraph graph2 = (StandardJanusGraph) JanusConfiguredGraphFactory.create("graph2");

        assertNotNull(graph1);
        assertNotNull(graph2);

        assertEquals(graph1.getConfiguration().getConfiguration().get(GRAPH_NAME), "graph1");        
        assertEquals(graph2.getConfiguration().getConfiguration().get(GRAPH_NAME), "graph2");        
        
        configGraphManagement.removeTemplateConfiguration();
        configGraphManagement.removeConfiguration("graph1");
        configGraphManagement.removeConfiguration("graph2");
        JanusGraphFactory.close("graph1");
        JanusGraphFactory.close("graph2");
           
    }
}

