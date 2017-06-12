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
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.ConfiguredGraphFactory;

import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.function.Function;
import java.util.Set;
import java.util.Map;
import java.io.IOException;

import javax.script.SimpleBindings;
import javax.script.Bindings;

public class JanusGraphManager implements GraphManager {

    private ConcurrentHashMap<String, Graph> graphs = new ConcurrentHashMap<String, Graph>();
    private ConcurrentHashMap<String, TraversalSource> traversalSources = new ConcurrentHashMap<String, TraversalSource>();
    private Settings settings = null;
    private Object instantiateGraphLock = new Object();

    private static JanusGraphManager instance = null;

    public JanusGraphManager(Settings settings) {
        if (null != this.instance) {
            throw new RuntimeException("You may not instantiate a JanusGraphManager. The single instance should be handled by Tinkerpop's GremlinServer startup processes.");
        }

        this.settings = settings;
        this.instance = this;
        // Open graphs defined at server start in settings.graphs
        settings.graphs.forEach((key, value) -> {
            StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(value, key);
            if (key.toLowerCase().equals("JanusConfigurationGraph".toLowerCase())) {
                new ConfigurationManagementGraph(graph);
            }
        });
    }

    public static JanusGraphManager getInstance() {
        return instance;
    }

    /**
     * @Deprecated
     */
    @Deprecated
    public ConcurrentHashMap getGraphs() {
        return graphs;
    }

    public Set<String> getGraphNames() {
        return graphs.keySet();
    }

    public Graph getGraph(String gName) {
        return graphs.get(gName);
    }

    public void putGraph(String gName, Graph g) {
        graphs.put(gName, g);
    }

    /**
     * @Deprecated
     */
    public ConcurrentHashMap getTraversalSources() {
        return traversalSources;
    }

    public Set<String> getTraversalSourceNames() {
        return traversalSources.keySet();
    }

    public TraversalSource getTraversalSource(String tsName) {
        return null; // JanusGraph do not support storing references to traversalSources
    }

    public void putTraversalSource(String tsName, TraversalSource ts) {
        throw new RuntimeException("JanusGraph does not support TraversalSource reference tracking.");
    }

    public TraversalSource removeTraversalSource(String tsName) {
        throw new RuntimeException("JanusGraph does not support TraversalSource reference tracking.");
    }

    public Bindings getAsBindings() {
        Bindings bindings = new SimpleBindings();
        graphs.forEach(bindings::put);
        return bindings;
    }

    public void rollbackAll() {
        graphs.forEach((key, value) -> {
            Graph graph = value;
            if (graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, false);
    }

    public void commitAll() {
        graphs.forEach((key, value) -> {
            Graph graph = value;
            if (graph.tx().isOpen())
                graph.tx().commit();
        });
    }

    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, true);
    }

    public void commitOrRollback(Set<String> graphSourceNamesToCloseTxOn, Boolean commit) {
        graphSourceNamesToCloseTxOn.forEach(e -> {
            Graph graph = getGraph(e);
            if (null != graph) {
                closeTx(graph, commit);
            }
        });
    }

    public void closeTx(Graph graph, Boolean commit) {
        if (graph.tx().isOpen()) {
            if (commit) {
                graph.tx().commit();
            } else {
                graph.tx().rollback();
            }
        }
    }

    public Graph openGraph(String gName, Function<String, Graph> thunk) {
        Graph graph = graphs.get(gName);
        if (graph != null) {
            return graph;
        } else {
            synchronized (instantiateGraphLock) {
                graph = graphs.get(gName);
                if (graph == null) {
                    try {
                        graph = thunk.apply(gName);
                        graphs.put(gName, graph);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return graph;
        }
    }

    public Graph removeGraph(String gName) {
        if (gName == null) return null;
        return graphs.remove(gName);
    }
}

