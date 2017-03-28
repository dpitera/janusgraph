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

import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.graphdb.management.ConfigurationGraphManagement;
import org.janusgraph.graphdb.management.utils.ConfigurationGraphManagementNotEnabled;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.function.Function;
import java.util.Set;

import javax.script.SimpleBindings;
import javax.script.Bindings;

public class JanusGraphManager implements GraphManager {

    private ConcurrentHashMap<String, Graph> graphs = new ConcurrentHashMap<String, Graph>();
    private ConcurrentHashMap<String, TraversalSource> traversalSources = new ConcurrentHashMap<String, TraversalSource>();
    private HashMap<String, GraphLock> graphOpenLockMap = new HashMap<String, GraphLock>();
    private Settings settings = null;

    private static JanusGraphManager instance = null;

    public JanusGraphManager(Settings settings) {
        if (null != this.instance) {
            throw new RuntimeException("You may not instantiate a JanusGraphManager. The single instance should be handled by Tinkerpop's GremlinServer startup processes.");
        }
        
        this.settings = settings;
        this.instance = this;
        // Open graphs defined at server start in settings.graphs
        settings.graphs.forEach((key, value) -> {
            StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(value);
            // Instantiate ConfigurationGraphManagement with graph config defined at server start
            if (key.equals("JanusConfigurationGraph")) {
                new ConfigurationGraphManagement(graph);
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

    public void addGraph(String gName, Graph g) {
        graphs.put(gName, g);
    }

    public ConcurrentHashMap getTraversalSources() {
        return traversalSources;
    }

    public TraversalSource getTraversalSource(String tsName) {
        return null; // JanusGraph do not support storing references to traversalSources
    }

    public void addTraversalSource(String tsName, TraversalSource ts) {
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
            GraphLock graphOpenLock = null;
            synchronized (graphOpenLockMap) {
                graphOpenLock = graphOpenLockMap.get(gName);
                if (graphOpenLock != null) {
                    if (graphOpenLock.getAvailable()) {
                        graphOpenLock.inc();
                    } else {
                        throw new RuntimeException("Graph %s has been deleted.".format(gName));
                    }
                } else {
                    GraphLock newLock = new GraphLock();
                    graphOpenLockMap.put(gName, newLock);
                    newLock.inc();
                    graphOpenLock = newLock;
                }
            }

            synchronized (graphOpenLock) {
                graph = graphs.get(gName);
                if (graph == null) {
                    Graph newGraph = thunk.apply(gName);
                    graph = newGraph;
                    graphs.put(gName, graph);
                }
            }

            synchronized (graphOpenLockMap) {
                graphOpenLock.dec();
                if ((graphOpenLock.getRefs() == 1) && (!graphOpenLock.getAvailable())) {
                    notify();
                }
                if (graphOpenLock.getRefs() == 0) {
                    graphOpenLockMap.remove(gName);
                }
            }
            return graph;
        }
    }

    public Graph removeGraph(String gName) throws InterruptedException {
        if (gName == null) return null;

        GraphLock graphOpenLock = null;
        synchronized (graphOpenLockMap) {
            graphOpenLock = graphOpenLockMap.get(gName);
            if (graphOpenLock != null) {
                graphOpenLock.inc();
                graphOpenLock.setAvailable(false);
            } else {
                GraphLock newLock = new GraphLock();
                newLock.inc();
                newLock.setAvailable(false);
                graphOpenLockMap.put(gName, newLock);
                graphOpenLock = newLock;
            }
            // Wait until the final reference counter is held by the deleter 
            wait();
        }
        
        // Graph has been marked as unavailable, and we are final thread holding a referene
        // so there is no need for thread-safe deletions or lock cleanup.
        Graph graph = graphs.remove(gName);
        if (null == graph) return null;
        try {
            ConfigurationGraphManagement.getInstance().removeConfiguration(gName);
        } catch (ConfigurationGraphManagementNotEnabled e) {
            // Do nothing because the server has not been enabled to use the
            // ConfigurationGraphManagement capabilities 
        }
        ((StandardJanusGraph) graph).close();

        graphOpenLock.dec();
        graphOpenLockMap.remove(gName);
        return graph;
    }

    private class GraphLock {
        private int refs = 0;
        private boolean available = true;

        public void inc() {
            refs++;
        }

        public void dec() {
            refs--;
        }

        public int getRefs() {
            return refs;
        }

        public void setAvailable(boolean available) {
            this.available = available;
        }

        public boolean getAvailable() {
            return available;
        }
    }
}

