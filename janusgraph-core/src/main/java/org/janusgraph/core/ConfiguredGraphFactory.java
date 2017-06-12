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

import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.diskstorage.StandardStoreManager;

import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.HashMap;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;

/**
 * This class allows for static methods to: 1) create graph references denoted by a {@link String}
 * graphName using a previously created template configuration using the
 * {@link ConfigurationManagementGraph} or 2) open a graph references denoted by a {@link String}
 * graphName for graphs that have been previously created or for graphs that have corresponding
 * previously created configurations created using the {@link ConfigurationManagementGraph}; this
 * class also defines a close which allows for removal of these {@link Graph} objects from the
 * {@link JanusGraphManager} reference tracker.
 */
public class ConfiguredGraphFactory {
    /**
     * Creates a {@link JanusGraph} configuration stored in {@ConfigurationGraphManagament}
     * configurationGraph and a {@link JanusGraph} graph reference according to the single
     * Template Configuration  previously created by the {@link ConfigurationManagementGraph} API;
     * if a configuration exists for this graph already, or a Template Configuration does not exist,
     * we throw a {@link RuntimeException}; If the Template Configuration does not include its
     * backend's respective keyspace/table/storage_directory parameter, we set the keyspace/table
     * to the {@link String} supplied graphName or we append the graphName to the supplied
     * storage_root parameter.
     *
     * @param String graphName
     *
     * @return JanusGraph
     */
    public static synchronized JanusGraph create(final String graphName) {
        ConfigurationManagementGraph configGraphManagement = getConfigGraphManagementInstance();

        Map<String, Object> graphConfigMap = configGraphManagement.getConfiguration(graphName);
        if (null != graphConfigMap) throw new RuntimeException(String.format("Configuration for graph %s already exists.", graphName));
        Map<String, Object> templateConfigMap = configGraphManagement.getTemplateConfiguration();
        if (null == templateConfigMap) {
            throw new RuntimeException("Please create a template Configuration using the ConfigurationManagementGraph#createTemplateConfiguration API.");
        }

        templateConfigMap.put(ConfigurationManagementGraph.PROPERTY_GRAPH_NAME, graphName);
        templateConfigMap.put(ConfigurationManagementGraph.PROPERTY_CREATED_USING_TEMPLATE, true);

        JanusGraph g;
        try {
            g = (JanusGraph) JanusGraphManager.getInstance().openGraph(graphName, (String gName) -> {
                return new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(new MapConfiguration(templateConfigMap))));
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        configGraphManagement.createConfiguration(new MapConfiguration(templateConfigMap));
        return g;
    }

    /**
     * Open a {@link JanusGraph} using a previously created Configuration using the
     * {@link ConfigurationManagementGraph} API; if a corresponding configuration does not exist, we throw
     * a {@link RuntimeException}, else return the Graph;
     *
     * NOTE: If your configuration corresponding to this graph does not contain information about
     * the backend's keyspace/table/storage directory, then we set the keyspace/table to the
     * graphName or set the storage directory to the storage_root + /graphName.
     */
    public static JanusGraph open(String graphName) {
        ConfigurationManagementGraph configGraphManagement = getConfigGraphManagementInstance();
        Map<String, Object> graphConfigMap = configGraphManagement.getConfiguration(graphName);
        if (null == graphConfigMap) {
            throw new RuntimeException("Please create configuration for this graph using the ConfigurationManagementGraph#createConfiguration API.");
        }
        return (JanusGraph) JanusGraphManager.getInstance().openGraph(graphName, (String gName) -> {
            return new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(new MapConfiguration(graphConfigMap))));
        });
    }

    /**
     * Removes the graph corresponding to the supplied {@link String} graphName
     * from the {@link JanusGraphManager} {@link Map<String, Graph} graph reference tracker and
     * returns the corresponding Graph, or null if it doesn't exist.
     *
     * @param configuration Graph
     * @return JanusGraph
     */
    public static JanusGraph close(String graphName) {
        return (JanusGraph) JanusGraphManager.getInstance().removeGraph(graphName);
    }

    /**
     * Returns a {@link MapConfiguration} corresponding to supplied .properties file
     * which lives at the supplied {@link String} fileLocation.
     *
     * @param {@link String} properties file location
     *
     * @return MapConfiguration
     */
    public static MapConfiguration fileToMapConfiguration(final String fileLocation) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File(fileLocation)));
        Map<String, Object> map = new HashMap<String, Object>();
        properties.entrySet().stream().forEach(entry -> {
            map.put((String) entry.getKey(), entry.getValue());
        });
        return new MapConfiguration(map);
    }

    private static ConfigurationManagementGraph getConfigGraphManagementInstance() {
        ConfigurationManagementGraph configGraphManagement = null;
        try {
            configGraphManagement = ConfigurationManagementGraph.getInstance();
        } catch (ConfigurationManagementGraphNotEnabledException e) {
            throw new RuntimeException(e);
        }
        return configGraphManagement;
    }
}

