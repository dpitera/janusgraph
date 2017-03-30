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
import org.janusgraph.graphdb.management.utils.ConfigurationGraphManagementNotEnabled;
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

public class ConfigurationGraphManagement {
    private static final Logger log = LoggerFactory.getLogger(ConfigurationGraphManagement.class);

    public static final String GRAPH_NAME = "ConfigurationGraph";
    private static ConfigurationGraphManagement instance = null;
    private final StandardJanusGraph graph;

    private final String VERTEX_LABEL = "Configuration";
    public static final String PROPERTY_GRAPH_NAME = "graph.graphname";
    private final String GRAPH_NAME_INDEX = "Graph_Name_Index";

    private final String PROPERTY_TEMPLATE = "Template_Configuration";
    private final String TEMPLATE_INDEX = "Template_Index";

    public ConfigurationGraphManagement(StandardJanusGraph graph) {
        if (null != instance) {
            throw new RuntimeException("ConfigurationGraphManagement should be instantiated just once, by the JanusGraphManager.");
        }

        this.graph = graph;
        instance = this;

        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        // create unique index on PROPERTY_GRAPH_NAME if it doesn't exist
        if (null == mgmt.getGraphIndex(GRAPH_NAME_INDEX)) {
            PropertyKey name = mgmt.makePropertyKey(PROPERTY_GRAPH_NAME).dataType(String.class).make();
            JanusGraphIndex index = mgmt.buildIndex(GRAPH_NAME_INDEX, Vertex.class).addKey(name).unique().buildCompositeIndex();
            try {
                if (index.getIndexStatus(name) == INSTALLED) {
                    ManagementSystem.awaitGraphIndexStatus(graph, GRAPH_NAME_INDEX).call();
                    mgmt.updateIndex(index, ENABLE_INDEX).get();
                } else if (index.getIndexStatus(name) == REGISTERED) {
                    mgmt.updateIndex(index, ENABLE_INDEX).get();                    
                }
            } catch (InterruptedException | ExecutionException e) {
                // log
                mgmt.rollback();
                graph.tx().rollback();
            }
            mgmt.commit();
            graph.tx().commit();
        }
        mgmt = graph.openManagement();
        // create index on PROPERTY_TEMPLATE if it doesn't exist
        if (null == mgmt.getGraphIndex(TEMPLATE_INDEX)) {
            PropertyKey template = mgmt.makePropertyKey(PROPERTY_TEMPLATE).dataType(Boolean.class).make();
            JanusGraphIndex index = mgmt.buildIndex(TEMPLATE_INDEX, Vertex.class).addKey(template).buildCompositeIndex();
            try {
                if (index.getIndexStatus(template) == INSTALLED) {
                    ManagementSystem.awaitGraphIndexStatus(graph, TEMPLATE_INDEX).call();
                    mgmt.updateIndex(index, ENABLE_INDEX).get();
                } else if (index.getIndexStatus(template) == REGISTERED) {
                    mgmt.updateIndex(index, ENABLE_INDEX).get();                    
                }
            } catch (InterruptedException | ExecutionException e) {
                // log
                mgmt.rollback();
                graph.tx().rollback();
            }
            mgmt.commit();
            graph.tx().commit();
        }
    }

    /**
     * If one of your "graphs" key was equivalent to \"JanusConfigurationGraph\" in your
     * YAML file supplied at server start, then we return the ConfigurationGraphManagement
     * Singleton-- otherwise we throw a {@link ConfigurationGraphManagementNotEnabled} exception.
     */
    public static ConfigurationGraphManagement getInstance() throws ConfigurationGraphManagementNotEnabled {
        if (null == instance) {
            throw new ConfigurationGraphManagementNotEnabled(
                "Please supply a graphs key equivalent to \"JanusConfigurationGraph\" in your " +
                "YAML file and restart the server to be able to use the functionality of the " +
                "ConfigurationGraphManagement class."
            );
        }

        return instance;
    }

    /**
     * Create a configuration according to the supplied {@link MapConfiguration}; you must include
     * the property "graph.graphname" with a {@link String} value in the configuration; you can then
     * open your {@link JanusGraph} using {@link String} graph.graphname without having to supply the
     * a Configuration or File each time using the {@JanusConfiguredGraphFactory}.
     */
    public void createConfiguration(final MapConfiguration config) {
        if (!config.containsKey(PROPERTY_GRAPH_NAME)) {
            throw new RuntimeException(
                String.format("Please include in your configuration the property \"%s\".",
                              PROPERTY_GRAPH_NAME
                )
            );
        }
        final Map<String, Object> map = config.getMap();
        Vertex v = graph.addVertex(T.label, VERTEX_LABEL);
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
     * JanusConfiguredGraphFactory} create signature and supplying a new {@link String} graphName.
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
        v.property(PROPERTY_TEMPLATE, true);
        map.forEach((key, value) -> {
            v.property(key, value);
        });
        graph.tx().commit();

    }

    /**
     * Update configuration corresponding to supplied {@link String} graphName; fully replace all
     * previous properties with those supplied in {@link MapConfiguration}; The supplied {@link MapConfiguration} must include a
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
        removeConfiguration(graphName, false); // do not commit here; we want to commit after full transaction completes
        createConfiguration(config);
    }

    /**
     * Update Template_Configuration by fully replacing all properties with supplied {@link
     * MapConfiguration}; your updated Configuration may not contain the property "graph.graphname";
     * NOTE: Any graph using a configuration that was created using the Template_Configuration must
     * be closed and reopened on every Janus Graph Node before the updated configuration is
     * guaranteed to be used.
     */
    public void updateTemplateConfig(final MapConfiguration config) {
        if (config.containsKey(PROPERTY_GRAPH_NAME)) {
            throw new RuntimeException(
                String.format("Your updated template configuration may not contain the property \"%s\".",
                              PROPERTY_GRAPH_NAME
                )
            );
        }
        log.warn("Any graph configuration created using the Template_Configuration are only guraranteed to have their configuration updated" +
                 "according to this new Template_Configuration when all the graph in question has been closed and reopened on every Janus Graph Node.");
        removeTemplateConfiguration(false); // do not commit here; we want to commit after full transaction completes
        createTemplateConfiguration(config);
    }

    /**
     * Remove Configuration according to {@link String graphName}
     */
    public void removeConfiguration(final String graphName) {
        removeConfiguration(graphName, true);
    }

    private void removeConfiguration(final String graphName, boolean commit) {
        Vertex v;
        try {
            v = graph.traversal().V().has(PROPERTY_GRAPH_NAME, graphName).next();
        } catch (FastNoSuchElementException e) {
            return;
        }
        v.remove();
        if (commit) graph.tx().commit();
    }

    /**
     * Remove Template_Configuration
     */
    public void removeTemplateConfiguration() {
        removeTemplateConfiguration(true);
    }

    private void removeTemplateConfiguration(boolean commit) {
        Vertex v;
        try {
            v = graph.traversal().V().has(PROPERTY_TEMPLATE, true).next();
        } catch (FastNoSuchElementException e) {
            return;
        }
        v.remove();
        if(commit) graph.tx().commit();
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
        // Note: there cannot be several vertices with the same configName property,
        // because we create this index using a unique constraint.
        return l.get(0);
    }

    /**
     * Get Template_Configuration if exists, else return null.
     *
     * @return Map<String, Object>
     */
    public Map<String, Object> getTemplateConfiguration() {
      List<Map<String, Object>> l = graph.traversal().V().has(PROPERTY_TEMPLATE, true).valueMap().toList();
      if (l.size() == 0) return null;
      // Note: there cannot be several Template_Configuration vertices as this graph is
      // inaccessible and there is no API to add additional ones.
      return l.get(0);
    }

    //public MapConfiguration fileToMapConfiguration(String fileLocation) {
    //
    //}
}

