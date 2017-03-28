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

public class JanusConfiguredGraphFactory {
    private static ConfigOption<String> CASSANDRA_CONFIG = GraphDatabaseConfiguration.CASSANDRA_KEYSPACE;
    private static String CASSANDRA_KEYSPACE = CASSANDRA_CONFIG.toStringWithoutRoot();
    
    private static ConfigOption<String> HBASE_CONFIG = GraphDatabaseConfiguration.HBASE_TABLE;
    private static String HBASE_TABLE = HBASE_CONFIG.toStringWithoutRoot();
    
    private static ConfigOption<String> DIR_CONFIG = GraphDatabaseConfiguration.STORAGE_DIRECTORY;
    private static String STORAGE_DIRECTORY = DIR_CONFIG.toStringWithoutRoot();
    
    private static ConfigOption<String> ROOT_CONFIG = GraphDatabaseConfiguration.STORAGE_ROOT;
    private static String STORAGE_ROOT = ROOT_CONFIG.toStringWithoutRoot(); 
    
    /**
     * Creates a {@link JanusGraph} configuration stored in {@ConfigurationGraphManagament}
     * configurationGraph and a {@link JanusGraph} graph reference according to the single
     * Template_Configuration previously created by the {@link ConfigurationGraphManagement} API;
     * if a configuration exists for this graph already, or a Template_Configuration does not exist,
     * we throw a {@link RuntimeException}; If the Template_Configuration does not include its
     * backend's respective keyspace/table/storage_directory parameter, we set the keyspace/table
     * to the {@link String} supplied graphName or we append the graphName to the supplied
     * storage_root parameter.
     * @param String graphName
     * 
     * @return JanusGraph
     */
    public static JanusGraph create(final String graphName) {
        return (JanusGraph) JanusGraphManager.getInstance().openGraph(graphName, (String gName) -> {
            ConfigurationGraphManagement configGraphManagement = null;
            try {
                configGraphManagement= ConfigurationGraphManagement.getInstance();
            } catch(ConfigurationGraphManagementNotEnabled e) {
                throw new RuntimeException(e);  
            }
            Map<String, Object> graphConfigMap = configGraphManagement.getConfiguration(gName);
            if (null != graphConfigMap) throw new RuntimeException(String.format("Configuration for graph %s already exists.", graphName));
            Map<String, Object> templateConfigMap = configGraphManagement.getTemplateConfiguration();
            if (null == templateConfigMap) throw new RuntimeException("Please create a template Configuration using the ConfigurationGraphManagement API.");
            
            templateConfigMap.put(ConfigurationGraphManagement.PROPERTY_GRAPH_NAME, gName);
            configGraphManagement.createConfiguration(new MapConfiguration(templateConfigMap));

            //if there is no keyspace or table or storage_directory, add here
            Map<String, Object> updatedMap = mutateMapBasedOnBackendAndGraphName(templateConfigMap, gName);

            return new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(new MapConfiguration(updatedMap))));
        });
    }

    /**
     * Open a {@link JanusGraph} using a previously created Configuration using the {@link
     * ConfigurationGraphManagement} API; if a corresponding configuration does not exist, we throw
     * a {@link RuntimeException}, else return the Graph;
     * NOTE: If you configuration corresponding to this graph does not contain information about
     * the backend's keyspace/table/storage directory, then we set the keyspace/table to the
     * graphName or set the storage directory to the storage_root + /graphName.
     */
    public static JanusGraph open(String graphName) {
        return (JanusGraph) JanusGraphManager.getInstance().openGraph(graphName, (String gName) -> {
            ConfigurationGraphManagement configGraphManagement = null;
            try {
                configGraphManagement = ConfigurationGraphManagement.getInstance();
            } catch (ConfigurationGraphManagementNotEnabled e) {
                throw new RuntimeException(e);
            }
            Map<String, Object> graphConfigMap = configGraphManagement.getConfiguration(gName);
            if (null == graphConfigMap) throw new RuntimeException("Please create configuration for this graph using the ConfigurationGraphManagement API.");
            
            // if there is no keyspace or table or storage_directory, add here
            Map<String, Object> updatedMap = mutateMapBasedOnBackendAndGraphName(graphConfigMap, gName);
  
            return new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(new MapConfiguration(updatedMap))));
        }); 
    }

    private static Map<String, Object> mutateMapBasedOnBackendAndGraphName(final Map<String, Object> map, final String graphName) {
        String backend = (String) map.get(STORAGE_BACKEND);
        if ((StandardStoreManager.getAllCassandraShorthands().contains(backend)) && (!map.containsKey(CASSANDRA_KEYSPACE))) {
            map.put(CASSANDRA_KEYSPACE, graphName);
        }
        if ((StandardStoreManager.getAllHbaseShorthands().contains(backend)) && (!map.containsKey(HBASE_TABLE))) {
            map.put(HBASE_TABLE, graphName);
        }
        if ((StandardStoreManager.getAllBerkeleyShorthands().contains(backend)) && (!map.containsKey(STORAGE_DIRECTORY))) {
            map.put(STORAGE_DIRECTORY, (String) map.get(STORAGE_ROOT) + graphName);
        }
        return map;
    }
}

