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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.configuration.*;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.ConfigurationGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

import org.janusgraph.graphdb.log.StandardLogProcessorFramework;
import org.janusgraph.graphdb.log.StandardTransactionLogProcessor;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * JanusGraphFactory is used to open or instantiate a JanusGraph graph database.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see JanusGraph
 */

public class JanusGraphFactory {

    private static final Logger log =
            LoggerFactory.getLogger(JanusGraphFactory.class);

    /**
     * Opens a {@link JanusGraph} database.
     * <p/>
     * If the argument points to a configuration file, the configuration file is loaded to configure the JanusGraph graph
     * If the string argument is a configuration short-cut, then the short-cut is parsed and used to configure the returned JanusGraph graph.
     * <p />
     * A configuration short-cut is of the form:
     * [STORAGE_BACKEND_NAME]:[DIRECTORY_OR_HOST]
     *
     * @param shortcutOrFile Configuration file name or configuration short-cut
     * @return JanusGraph graph database configured according to the provided configuration
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/configuration.html">"Configuration" manual chapter</a>
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/titan-config-ref.html">Configuration Reference</a>
     */
    public static JanusGraph open(String shortcutOrFile) {
        return open(getLocalConfiguration(shortcutOrFile));
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/configuration.html">"Configuration" manual chapter</a>
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/titan-config-ref.html">Configuration Reference</a>
     */
    public static JanusGraph open(Configuration configuration) {
        return open(new CommonsConfiguration(configuration));
    }

    /** Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
    */
    public static JanusGraph open(BasicConfiguration configuration) {
        return open(configuration.getConfiguration());
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     */
    public static JanusGraph open(ReadConfiguration configuration) {
        ModifiableConfiguration config = new ModifiableConfiguration(ROOT_NS, (WriteConfiguration) configuration, BasicConfiguration.Restriction.NONE);
        if (config.has(GRAPH_NAME)) {
            String graphName = config.get(GRAPH_NAME);
            String backend = config.get(STORAGE_BACKEND);

            // Update keyspace/table/storage_directory acc. to graph_name if not supplied
            List<String> cassandra = StandardStoreManager.getAllCassandraShorthands();
            if (cassandra.contains(backend) && !config.has(CASSANDRA_KEYSPACE)) {
                config.set(CASSANDRA_KEYSPACE, graphName);
            }
            List<String> hbase = StandardStoreManager.getAllHbaseShorthands();
            if (hbase.contains(backend) && !config.has(HBASE_TABLE)) {
                config.set(HBASE_TABLE, graphName);
            }
            List<String> berkeley = StandardStoreManager.getAllBerkeleyShorthands();
            if (berkeley.contains(backend) && !config.has(STORAGE_DIRECTORY)) {
                config.set(STORAGE_DIRECTORY, config.get(STORAGE_ROOT) + "/" + graphName);
            }

            return (JanusGraph) JanusGraphManager.getInstance().openGraph(graphName, (gName) -> {
                return new StandardJanusGraph(new GraphDatabaseConfiguration(configuration));
            });
        } else {
            return new StandardJanusGraph(new GraphDatabaseConfiguration(configuration));
        }
    }

    /** 
     * Closes a {@link JanusGraph} graph
     * 
     * @param configuration Graph
     * @return JanusGraph
     */
    public static JanusGraph close(Graph graph) throws Exception {
        Graph g = JanusGraphManager.getInstance().removeGraph(((StandardJanusGraph) graph).getGraphName());
        if (g == null) { //this graph reference is not being tracked by JanusGraphManager reference tracker
            graph.close();
            return (JanusGraph) graph;
        }
        return (JanusGraph) g;
    }

    /**
     * Returns a {@link Builder} that allows to set the configuration options for opening a JanusGraph graph database.
     * <p />
     * In the builder, the configuration options for the graph can be set individually. Once all options are configured,
     * the graph can be opened with {@link org.janusgraph.core.JanusGraphFactory.Builder#open()}.
     *
     * @return
     */
    public static Builder build() {
        return new Builder();
    }

    //--------------------- BUILDER -------------------------------------------

    public static class Builder {

        private final WriteConfiguration writeConfiguration;

        private Builder() {
            writeConfiguration = new CommonsConfiguration();
        }

        /**
         * Configures the provided configuration path to the given value.
         *
         * @param path
         * @param value
         * @return
         */
        public Builder set(String path, Object value) {
            writeConfiguration.set(path, value);
            return this;
        }

        /**
         * Opens a JanusGraph graph with the previously configured options.
         *
         * @return
         */
        public JanusGraph open() {
            ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                    writeConfiguration.copy(), BasicConfiguration.Restriction.NONE);
            return JanusGraphFactory.open(mc);
        }


    }

    /**
     * Returns a {@link org.janusgraph.core.log.LogProcessorFramework} for processing transaction log entries
     * against the provided graph instance.
     *
     * @param graph
     * @return
     */
    public static LogProcessorFramework openTransactionLog(JanusGraph graph) {
        return new StandardLogProcessorFramework((StandardJanusGraph)graph);
    }

    /**
     * Returns a {@link TransactionRecovery} process for recovering partially failed transactions. The recovery process
     * will start processing the write-ahead transaction log at the specified transaction time.
     *
     * @param graph
     * @param start
     * @return
     */
    public static TransactionRecovery startTransactionRecovery(JanusGraph graph, Instant start) {
        return new StandardTransactionLogProcessor((StandardJanusGraph)graph, start);
    }

    //###################################
    //          HELPER METHODS
    //###################################

    private static ReadConfiguration getLocalConfiguration(String shortcutOrFile) {
        File file = new File(shortcutOrFile);
        if (file.exists()) return getLocalConfiguration(file);
        else {
            MapConfiguration config = new MapConfiguration(new HashMap<String, Object>());
            Map<String, Object> map = config.getMap();
            String backend = null;
            String ipsOrDirectory = null;
            String[] parts = shortcutOrFile.split(":");

            String graphNameProp = GRAPH_NAME.toStringWithoutRoot();
            if (parts.length == 1) {
                backend = parts[0];
            } else if ((parts.length == 2) && (parts[1].equals("inmemory"))) {
                String graphName = parts[0];
                backend = parts[1];
                map.put(graphNameProp, graphName);
            } else if (parts.length == 2) {
                backend = parts[0];
                ipsOrDirectory = parts[1];
            } else if (parts.length == 3) {
                String graphName = parts[0];
                backend = parts[1];
                ipsOrDirectory = parts[2];
                map.put(graphNameProp, graphName);
            } else {
                throw new RuntimeException("Please supply a string of the form \"inmemory\" or \"<graph>:inmemory\" or \"<backend>:<ipsOrDirectory>\" or \"<graph>:<backend>:<ipsOrDirectory>\".");
            }
            Preconditions.checkArgument(StandardStoreManager.getAllManagerClasses().containsKey(backend.toLowerCase()), "Backend shorthand unknown: %s", backend);
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), backend);
            ConfigOption option = Backend.getOptionForShorthand(backend);
            if (option==null) {
                Preconditions.checkArgument(ipsOrDirectory==null);
            } else if (option==STORAGE_DIRECTORY) {
                Preconditions.checkArgument(StringUtils.isNotBlank(ipsOrDirectory),"Need to provide additional argument to initialize storage backend");
                map.put(STORAGE_DIRECTORY.toStringWithoutRoot(), getAbsolutePath(ipsOrDirectory));
            } else if (option==STORAGE_CONF_FILE) {
                Preconditions.checkArgument(StringUtils.isNotBlank(ipsOrDirectory),"Need to provide additional argument to initialize storage backend");
                map.put(STORAGE_CONF_FILE.toStringWithoutRoot(), getAbsolutePath(ipsOrDirectory));
            } else if (option==STORAGE_HOSTS) {
                Preconditions.checkArgument(StringUtils.isNotBlank(ipsOrDirectory),"Need to provide additional argument to initialize storage backend");
                map.put(STORAGE_HOSTS.toStringWithoutRoot(), new String[]{ipsOrDirectory});
            } else throw new IllegalArgumentException("Invalid configuration option for backend "+option);
            return new CommonsConfiguration(config);
        }
    }

    /**
     * Load a properties file containing a JanusGraph graph configuration.
     * <p/>
     * <ol>
     * <li>Load the file contents into a {@link org.apache.commons.configuration.PropertiesConfiguration}</li>
     * <li>For each key that points to a configuration object that is either a directory
     * or local file, check
     * whether the associated value is a non-null, non-absolute path. If so,
     * then prepend the absolute path of the parent directory of the provided configuration {@code file}.
     * This has the effect of making non-absolute backend
     * paths relative to the config file's directory rather than the JVM's
     * working directory.
     * <li>Return the {@link ReadConfiguration} for the prepared configuration file</li>
     * </ol>
     * <p/>
     *
     * @param file A properties file to load
     * @return A configuration derived from {@code file}
     */
    private static ReadConfiguration getLocalConfiguration(File file) {
        Preconditions.checkArgument(file != null && file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file, but was given: %s", file.toString());

        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(file);

            final File tmpParent = file.getParentFile();
            final File configParent;

            if (null == tmpParent) {
                /*
                 * null usually means we were given a JanusGraph config file path
                 * string like "foo.properties" that refers to the current
                 * working directory of the process.
                 */
                configParent = new File(System.getProperty("user.dir"));
            } else {
                configParent = tmpParent;
            }

            Preconditions.checkNotNull(configParent);
            Preconditions.checkArgument(configParent.isDirectory());

            // TODO this mangling logic is a relic from the hardcoded string days; it should be deleted and rewritten as a setting on ConfigOption
            final Pattern p = Pattern.compile("(" +
                    Pattern.quote(STORAGE_NS.getName()) + "\\..*" +
                            "(" + Pattern.quote(STORAGE_DIRECTORY.getName()) + "|" +
                                  Pattern.quote(STORAGE_CONF_FILE.getName()) + ")"
                    + "|" +
                    Pattern.quote(INDEX_NS.getName()) + "\\..*" +
                            "(" + Pattern.quote(INDEX_DIRECTORY.getName()) + "|" +
                                  Pattern.quote(INDEX_CONF_FILE.getName()) +  ")"
            + ")");

            final Iterator<String> keysToMangle = Iterators.filter(configuration.getKeys(), new Predicate<String>() {
                @Override
                public boolean apply(String key) {
                    if (null == key)
                        return false;
                    return p.matcher(key).matches();
                }
            });

            while (keysToMangle.hasNext()) {
                String k = keysToMangle.next();
                Preconditions.checkNotNull(k);
                String s = configuration.getString(k);
                Preconditions.checkArgument(StringUtils.isNotBlank(s),"Invalid Configuration: key %s has null empty value",k);
                configuration.setProperty(k,getAbsolutePath(configParent,s));
            }
            return new CommonsConfiguration(configuration);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + file, e);
        }
    }

    private static final String getAbsolutePath(String file) {
        return getAbsolutePath(new File(System.getProperty("user.dir")), file);
    }

    private static final String getAbsolutePath(final File configParent, String file) {
        File storedir = new File(file);
        if (!storedir.isAbsolute()) {
            String newFile = configParent.getAbsolutePath() + File.separator + file;
            log.debug("Overwrote relative path: was {}, now {}", file, newFile);
            return newFile;
        } else {
            log.debug("Loaded absolute path for key: {}", file);
            return file;
        }
    }

}
