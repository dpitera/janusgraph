// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core;

import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.diskstorage.BackendException;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.File;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.After;
import static org.junit.Assert.*;

public class ConfiguredGraphFactoryTest {
    private static JanusGraphManager gm;
    private static ConfigurationManagementGraph configGraphManagement;
    static {
        gm = new JanusGraphManager(new Settings());
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        MapConfiguration config = new MapConfiguration(map);
        StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(config)));
        configGraphManagement = new ConfigurationManagementGraph(graph);
    }

    @After
    public void cleanUp() {
        if (configGraphManagement.getTemplateConfiguration() != null) {
            configGraphManagement.removeTemplateConfiguration();
        }
    }

    @Test
    public void shouldGetConfigurationManagementGraphInstance() throws ConfigurationManagementGraphNotEnabledException {
        ConfigurationManagementGraph thisInstance = ConfigurationManagementGraph.getInstance();
        assertNotNull(thisInstance);
    }

    @Test
    public void shouldOpenGraphUsingConfiguration() throws InterruptedException, BackendException {
        try {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            configGraphManagement.createConfiguration(new MapConfiguration(map));
            StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);
        } finally {
            configGraphManagement.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void graphConfigurationShouldBeWhatWeExpect() throws InterruptedException, BackendException {
        try {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            configGraphManagement.createConfiguration(new MapConfiguration(map));
            StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);
            assertEquals(graph.getConfiguration().getConfiguration().get(GRAPH_NAME), "graph1");
            assertEquals(graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND), "inmemory");
        } finally {
            configGraphManagement.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void shouldCreateAndGetGraphUsingTemplateConfiguration() throws InterruptedException, BackendException {
        try {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            configGraphManagement.createTemplateConfiguration(new MapConfiguration(map));
            StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");

            assertNotNull(graph);
            assertEquals(graph, graph1);
        } finally {
            configGraphManagement.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void graphConfigurtionShouldBeWhatWeExpectWhenUsingTemplateConfiguration()
        throws InterruptedException, BackendException {

        try {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            configGraphManagement.createTemplateConfiguration(new MapConfiguration(map));
            StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");

            assertNotNull(graph);
            assertEquals(graph, graph1);
            assertEquals(graph.getConfiguration().getConfiguration().get(GRAPH_NAME), "graph1");
            assertEquals(graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND), "inmemory");

        } finally {
            configGraphManagement.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void shouldThrowConfigurationDoesNotExistError() {
        boolean errorThrown = false;
        try {
            ConfiguredGraphFactory.open("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }
        assertTrue(errorThrown);
    }

    @Test
    public void shouldThrowTemplateConfigurationDoesNotExistError() {
        boolean errorThrown = false;
        try {
            ConfiguredGraphFactory.create("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }
        assertTrue(errorThrown);
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
            ConfiguredGraphFactory.open("graph1");
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
            ConfiguredGraphFactory.create("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }

        assertTrue(errorThrown);
    }

    @Test
    public void shouldFailToOpenGraphAfterRemoveConfiguration() {
        boolean errorThrown = false;

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        configGraphManagement.createConfiguration(new MapConfiguration(map));
        configGraphManagement.removeConfiguration("graph1");
        try {
            ConfiguredGraphFactory.create("graph1");
        } catch (RuntimeException e) {
            errorThrown = true;
        }

        assertTrue(errorThrown);
    }

    @Test
    public void updateConfigurationShouldOnlyUpdateForGraphAfterWeCloseAndReOpen() throws InterruptedException, BackendException {
        try {
            boolean errorThrown = false;
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            configGraphManagement.createConfiguration(new MapConfiguration(map));
            StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);

            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "bogusBackend");
            configGraphManagement.updateConfiguration("graph1", new MapConfiguration(map));

            StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);

            ConfiguredGraphFactory.close("graph1");

            try {
                StandardJanusGraph graph2 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            } catch (Exception e) {
                // we should throw an error since the config has been updated and we are attempting
                // to open a bogus backend
                errorThrown = true;
            }
            assertTrue(errorThrown);
        } finally {
            configGraphManagement.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void shouldCreateTwoGraphsUsingSameTemplateConfiguration() throws InterruptedException, BackendException {
        try {Map<String, Object> map = new HashMap<String, Object>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            configGraphManagement.createTemplateConfiguration(new MapConfiguration(map));
            StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            StandardJanusGraph graph2 = (StandardJanusGraph) ConfiguredGraphFactory.create("graph2");

            assertNotNull(graph1);
            assertNotNull(graph2);

            assertEquals(graph1.getConfiguration().getConfiguration().get(GRAPH_NAME), "graph1");
            assertEquals(graph2.getConfiguration().getConfiguration().get(GRAPH_NAME), "graph2");
        } finally {
            configGraphManagement.removeConfiguration("graph1");
            configGraphManagement.removeConfiguration("graph2");
            ConfiguredGraphFactory.close("graph1");
            ConfiguredGraphFactory.close("graph2");
        }
    }

    @Test
    public void fileToMapConfigurationShouldReturnAllProperties() throws IOException {
        File file = new File(getClass().getClassLoader().getResource("org/janusgraph/core/janusgraph-testing.properties").getFile());
        String propertiesFileLocation = file.getAbsolutePath();
        MapConfiguration config = ConfiguredGraphFactory.fileToMapConfiguration(propertiesFileLocation);

        assertNotNull(config);

        Map<String, Object> map = config.getMap();

        assertNotNull(map);
        assertEquals(map.keySet().size(), 2);
        assertEquals(map.get("storage.backend"), "inmemory");
        assertEquals(map.get("graph.graphname"), "testgraph");
    }
}

