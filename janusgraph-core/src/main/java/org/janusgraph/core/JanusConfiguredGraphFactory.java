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

import org.janusgraph.graphdb.management.ConfigurationGraphManagement;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.graphdb.management.JanusGraphManager.*;
import static org.janusgraph.graphdb.management.ConfigurationGraphManagement.*;
import org.janusgraph.graphdb.management.utils.ConfigurationGraphManagementNotEnabled;
import org.janusgraph.diskstorage.StandardStoreManager;

import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.List;

public class JanusConfiguredGraphFactory {
    /**
     * Creates a {@link JanusGraph} configuration stored in {@ConfigurationGraphManagament}
     * configurationGraph and a {@link JanusGraph} graph reference according to the single
     * Template_Configuration previously created by the {@link ConfigurationGraphManagement} API;
     * if a configuration exists for this graph already, or a Template_Configuration does not exist,
     * we throw a {@link RuntimeException}; If the Template_Configuration does not include its
     * backend's respective keyspace/table/storage_directory parameter, we set the keyspace/table
     * to the {@link String} supplied graphName or we append the graphName to the supplied
     * storage_root parameter.
     *
     * @param String graphName
     *
     * @return JanusGraph
     */
    public static JanusGraph create(final String graphName) {
        ConfigurationGraphManagement configGraphManagement = null;
        try {
            configGraphManagement= ConfigurationGraphManagement.getInstance();
        } catch(ConfigurationGraphManagementNotEnabled e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> graphConfigMap = configGraphManagement.getConfiguration(graphName);
        if (null != graphConfigMap) throw new RuntimeException(String.format("Configuration for graph %s already exists.", graphName));
        Map<String, Object> templateConfigMap = configGraphManagement.getTemplateConfiguration();
        if (null == templateConfigMap) throw new RuntimeException("Please create a template Configuration using the ConfigurationGraphManagement API.");

        templateConfigMap.put(ConfigurationGraphManagement.PROPERTY_GRAPH_NAME, graphName);

        // Vertex property values are deserialized as Lists, so let's unserialize the response
        templateConfigMap.forEach((key, value) -> {
            if (value instanceof List) {
                templateConfigMap.put(key, ((List) value).get(0));
            }
        });

        configGraphManagement.createConfiguration(new MapConfiguration(templateConfigMap));

        // If there is no keyspace or table or storage_directory, add here
        Map<String, Object> updatedMap = mutateMapBasedOnBackendAndGraphName(templateConfigMap, graphName);
        JanusGraph g;
        try {
            g = (JanusGraph) JanusGraphManager.getInstance().openGraph(graphName, (String gName) -> {
                return new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(new MapConfiguration(updatedMap))));
            });
        } catch (Exception e) {
            configGraphManagement.removeConfiguration(graphName);
            throw new RuntimeException(e);
        }
        return g;
    }

    /**
     * Open a {@link JanusGraph} using a previously created Configuration using the
     * {@link ConfigurationGraphManagement} API; if a corresponding configuration does not exist, we throw
     * a {@link RuntimeException}, else return the Graph;
     *
     * NOTE: If you configuration corresponding to this graph does not contain information about
     * the backend's keyspace/table/storage directory, then we set the keyspace/table to the
     * graphName or set the storage directory to the storage_root + /graphName.
     */
    public static JanusGraph open(String graphName) {
        ConfigurationGraphManagement configGraphManagement = null;
        try {
            configGraphManagement = ConfigurationGraphManagement.getInstance();
        } catch (ConfigurationGraphManagementNotEnabled e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> graphConfigMap = configGraphManagement.getConfiguration(graphName);
        if (null == graphConfigMap) throw new RuntimeException("Please create configuration for this graph using the ConfigurationGraphManagement API.");

        // Vertex property values are deserialized as Lists, so let's unserialize the response
        graphConfigMap.forEach((key, value) -> {
            if (value instanceof List) {
                graphConfigMap.put(key, ((List) value).get(0));
            }
        });

        // if there is no keyspace or table or storage_directory, add here
        Map<String, Object> updatedMap = mutateMapBasedOnBackendAndGraphName(graphConfigMap, graphName);
        return (JanusGraph) JanusGraphManager.getInstance().openGraph(graphName, (String gName) -> {
            return new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(new MapConfiguration(updatedMap))));
        });
    }

    /**
     * Closes a {@link JanusGraph} graph by supplying {@link String} graphName
     * and removes the graph from the {@link JanusGraphManager} {@link Map<String, Graph}
     * graph reference tracker.
     *
     * @param configuration Graph
     * @return JanusGraph
     */
    public static JanusGraph close(String graphName) throws InterruptedException {
        return (JanusGraph) JanusGraphManager.getInstance().removeGraph(graphName);
    }
    
     
    // Exposed for unit Testing 
    protected static Map<String, Object> mutateMapBasedOnBackendAndGraphName(final Map<String, Object> map, final String graphName) {
        String backend = (String) map.get(STORAGE_BACKEND.toStringWithoutRoot());
        String cassandraKeyspace = CASSANDRA_KEYSPACE.toStringWithoutRoot();
        String hbaseTable = HBASE_TABLE.toStringWithoutRoot();
        String storageDir = STORAGE_DIRECTORY.toStringWithoutRoot();
        String storageRoot = STORAGE_ROOT.toStringWithoutRoot();

        if ((StandardStoreManager.getAllCassandraShorthands().contains(backend)) && (!map.containsKey(cassandraKeyspace))) {
            map.put(cassandraKeyspace, graphName);
        }
        if ((StandardStoreManager.getAllHbaseShorthands().contains(backend)) && (!map.containsKey(hbaseTable))) {
            map.put(hbaseTable, graphName);
        }
        if ((StandardStoreManager.getAllBerkeleyShorthands().contains(backend)) && (!map.containsKey(storageDir))) {
            map.put(storageDir, (String) map.get(storageRoot) + "/" + graphName);
        }
        return map;
    }
}

