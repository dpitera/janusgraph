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

package org.janusgraph.graphdb.management;

import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import static org.janusgraph.core.schema.SchemaAction.ENABLE_INDEX;
import static org.janusgraph.core.schema.SchemaAction.REGISTER_INDEX;
import static org.janusgraph.core.schema.SchemaStatus.INSTALLED;
import static org.janusgraph.core.schema.SchemaStatus.REGISTERED;
import static org.janusgraph.core.schema.SchemaStatus.ENABLED;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.ConversionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ConfigurationManagementGraph {

    public static final String PROPERTY_GRAPH_NAME = "graph.graphname";
    public static final String PROPERTY_CREATED_USING_TEMPLATE = "Created_Using_Template";

    private static final Logger log = LoggerFactory.getLogger(ConfigurationManagementGraph.class);
    private static ConfigurationManagementGraph instance = null;

    private final StandardJanusGraph graph;
    private final String vertexLabel = "Configuration";
    private final String graphNameIndex = "Graph_Name_Index";
    private final String propertyTemplate = "Template_Configuration";
    private final String templateIndex = "Template_Index";
    private final String propertyCreatedUsingTemplateIndex = "Created_Using_Template_Index";

    /**
     * This class allows you to create/update/remove configuration objects used to open a {@link Graph};
     * To use these APIs, you must define one of your graphs keys as "JanusConfigurationGraph"
     * in your server's YAML; the configuration objects you define using these APIs will be stored
     * in a graph representation (on a vertex, more precisely), and this graph will be opened
     * according to the file supplied along with the "JanusConfigurationGraph" key.
     */
    public ConfigurationManagementGraph(StandardJanusGraph graph) {
        if (null != instance) {
            throw new RuntimeException("ConfigurationManagementGraph should be instantiated just once, by the JanusGraphManager.");
        }

        this.graph = graph;
        instance = this;

        createIndexIfDoesNotExist(graphNameIndex, PROPERTY_GRAPH_NAME, String.class, true);
        createIndexIfDoesNotExist(templateIndex, propertyTemplate, Boolean.class, false);
        createIndexIfDoesNotExist(propertyCreatedUsingTemplateIndex, PROPERTY_CREATED_USING_TEMPLATE, Boolean.class, false);
    }

    /**
     * If one of your "graphs" key was equivalent to \"JanusConfigurationGraph\" in your
     * YAML file supplied at server start, then we return the ConfigurationManagementGraph
     * Singleton-- otherwise we throw a {@link ConfigurationManagementGraphNotEnabledException} exception.
     */
    public static ConfigurationManagementGraph getInstance() throws ConfigurationManagementGraphNotEnabledException {
        if (null == instance) {
            throw new ConfigurationManagementGraphNotEnabledException(
                "Please add a key named \"JanusConfigurationGraph\" to the \"graphs\" property " +
                "in your YAML file and restart the server to be able to use the functionality " +
                "of the ConfigurationManagementGraph class."
            );
        }

        return instance;
    }

    /**
     * Create a configuration according to the supplied {@link MapConfiguration}; you must include
     * the property "graph.graphname" with a {@link String} value in the configuration; you can then
     * open your {@link JanusGraph} using {@link String} graph.graphname without having to supply the
     * a Configuration or File each time using the {@JanusGraphConfiguredFactory}.
     */
    public void createConfiguration(final MapConfiguration config) {
        if (!config.containsKey(PROPERTY_GRAPH_NAME)) {
            throw new RuntimeException(
                String.format("Please include the property \"%s\" in your configuration.",
                              PROPERTY_GRAPH_NAME
                )
            );
        }
        final Map<String, Object> map = config.getMap();
        Vertex v = graph.addVertex(T.label, vertexLabel);
        map.forEach((key, value) -> {
            v.property(key, value);
        });
        graph.tx().commit();
    }

    /**
     * Create a "Template_Configuration" according to the supplied {@link MapConfiguration}; if
     * you already created a Template_Configuration or the supplied {@link MapConfiguration}
     * contains the property "graph.graphname", we throw a {@link RuntimeException}; you can then use
     * this Template_Configuration to create a {@JanusGraph} using the {@link
     * JanusGraphConfiguredFactory} create signature and supplying a new {@link String} graphName.
     */
    public void createTemplateConfiguration(MapConfiguration config) {
        if (config.containsKey(PROPERTY_GRAPH_NAME)) {
            throw new RuntimeException(
                String.format("Your template configuration may not contain the property \"%s\".",
                              PROPERTY_GRAPH_NAME
                )
            );
        }
        if (null != getTemplateConfiguration()) {
            throw new RuntimeException("You may only have one template configuration and one exists already.");
        }
        final Map<String, Object> map = config.getMap();
        Vertex v = graph.addVertex();
        v.property(propertyTemplate, true);
        map.forEach((key, value) -> {
            v.property(key, value);
        });
        graph.tx().commit();

    }

    /**
     * Update configuration corresponding to supplied {@link String} graphName; we update supplied existing
     * properties and add new ones to the {@link MapConfiguration}; The supplied {@link MapConfiguration} must include a
     * property "graph.graphname" and it must match supplied {@link String} graphName;
     * NOTE: The updated configuration is only guaranteed to take effect if the {@link Graph} corresponding to
     * {@link String} graphName has been closed and reopened on every Janus Graph Node.
     */
    public void updateConfiguration(final String graphName, final MapConfiguration config) {
        if (config.containsKey(PROPERTY_GRAPH_NAME)) {
            String graphNameOnConfig = (String) config.getMap().get(PROPERTY_GRAPH_NAME);
            if (!graphName.equals(graphNameOnConfig)) {
                throw new RuntimeException(
                    String.format("Supplied graphName %s does not match property value supplied on config: %s.",
                                  graphName,
                                  graphNameOnConfig
                    )
                );
            }
        } else {
            config.getMap().put(PROPERTY_GRAPH_NAME, graphName);
        }
        log.warn("Configuration {} is only guaranteed to take effect when graph {} has been closed and reopened on all Janus Graph Nodes.",
            graphName,
            graphName
        );
        updateVertexWithProperties(PROPERTY_GRAPH_NAME, graphName, config.getMap());
    }

    /**
     * Update Template_Configuration by updating supplied existing properties and adding new ones to the
 		 * {@link MapConfiguration}; your updated Configuration may not contain the property "graph.graphname";
     * NOTE: Any graph using a configuration that was created using the Template_Configuration must--
     * 1) be closed and reopened on every Janus Graph Node 2) have its corresponding Configuration removed
     * and 3) recreate the graph-- before the update is guaranteed to take effect.
     */
    public void updateTemplateConfiguration(final MapConfiguration config) {
        if (config.containsKey(PROPERTY_GRAPH_NAME)) {
            throw new RuntimeException(
                String.format("Your updated template configuration may not contain the property \"%s\".",
                              PROPERTY_GRAPH_NAME
                )
            );
        }
        log.warn("Any graph configuration created using the Template_Configuration are only guraranteed to have their configuration updated " +
                 "according to this new Template_Configuration when the graph in question has been closed on every Janus Graph Node, its " +
                 "corresponding Configuration has been removed, and the graph has been recreated.");
        updateVertexWithProperties(propertyTemplate, true, config.getMap());
    }


    /**
     * Remove Configuration according to {@link String graphName}
     */
    public void removeConfiguration(final String graphName) {
        Vertex v;
        try {
            v = graph.traversal().V().has(PROPERTY_GRAPH_NAME, graphName).next();
        } catch (FastNoSuchElementException e) {
            return;
        }
        v.remove();
        graph.tx().commit();
    }

    /**
     * Remove Template_Configuration
     */
    public void removeTemplateConfiguration() {
        Vertex v;
        try {
            v = graph.traversal().V().has(propertyTemplate, true).next();
        } catch (FastNoSuchElementException e) {
            return;
        }
        v.remove();
        graph.tx().commit();
    }

    /**
     * Get Configuration according to supplied {@link String} graphName mapped to a specific
     * {@link Graph}; if does not exist, return null.
     *
     * @return Map<String, Object>
     */
    public Map<String, Object> getConfiguration(final String configName) {
        List<Map<String, Object>> l = graph.traversal().V().has(PROPERTY_GRAPH_NAME, configName).valueMap().toList();
        if (l.size() == 0) return null;
        else if (l.size() > 1) { // this case shouldn't happen because our index has a unique constraint
            log.warn("Your configuration management graph is an a bad state. Please " +
                     "ensure you have just one configuration per graph. The behavior " +
                     "of the class' APIs are hereonout unpredictable until this is fixed.");
        }
        return deserializeVertexProperties(l.get(0));
    }

    /**
     * Get Template_Configuration if exists, else return null.
     *
     * @return Map<String, Object>
     */
    public Map<String, Object> getTemplateConfiguration() {
        List<Map<String, Object>> l = graph.traversal().V().has(propertyTemplate, true).valueMap().toList();
        if (l.size() == 0) return null;

        if (l.size() > 1) {
            log.warn("Your configuration management graph is an a bad state. Please " +
                     "ensure you have just one template configuration. The behavior " +
                     "of the class' APIs are hereonout unpredictable until this is fixed.");
        }
        l.get(0).remove(propertyTemplate);
        return deserializeVertexProperties(l.get(0));

    }

    private void createIndexIfDoesNotExist(String indexName, String propertyKeyName, Class dataType,boolean unique) {
        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        if (null == mgmt.getGraphIndex(indexName)) {
            PropertyKey key = mgmt.makePropertyKey(propertyKeyName).dataType(dataType).make();

            JanusGraphIndex index;
            if (unique) index = mgmt.buildIndex(indexName, Vertex.class).addKey(key).unique().buildCompositeIndex();
            else index = mgmt.buildIndex(indexName, Vertex.class).addKey(key).buildCompositeIndex();
            try {
                if (index.getIndexStatus(key) == INSTALLED) {
                    mgmt.commit();
                    ManagementSystem.awaitGraphIndexStatus(graph, indexName).call();
                    mgmt = graph.openManagement();
                    mgmt.updateIndex(index, ENABLE_INDEX).get();
                } else if (index.getIndexStatus(key) == REGISTERED) {
                    mgmt.updateIndex(index, ENABLE_INDEX).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.warn("Failed to create index {} for ConfigurationManagementGraph with exception: {}",
                        indexName,
                        e.toString()
                );
                mgmt.rollback();
                graph.tx().rollback();
            }
            mgmt.commit();
            graph.tx().commit();
        }
    }

    private void updateVertexWithProperties(String propertyKey, Object propertyValue, Map<String, Object> map) {
        if (graph.traversal().V().has(propertyKey, propertyValue).hasNext()) {
            Vertex v = graph.traversal().V().has(propertyKey, propertyValue).next();
            map.forEach((key, value) -> {
                v.property(key, value);
            });
            graph.tx().commit();
        }
    }

    private Map<String, Object> deserializeVertexProperties(Map<String, Object> map) {
        map.forEach((key, value) -> {
            if (value instanceof List) {
                if (((List) value).size() > 1) {
                    log.warn("Your configuration management graph is an a bad state. Please " +
                             "ensure each vertex property is not supplied a Collection as a value. The behavior " +
                             "of the class' APIs are hereonout unpredictable until this is fixed.");
                }
                map.put(key, ((List) value).get(0));
            }
        });
        return map;
    }
}

