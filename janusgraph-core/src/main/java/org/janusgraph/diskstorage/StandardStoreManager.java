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

package org.janusgraph.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap;

import java.util.*;

/**
 * This enum is only intended for use by JanusGraph internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardStoreManager {
    BDB_JE("org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager", "berkeleyje", "berkeley"),
    CASSANDRA_THRIFT("org.janusgraph.diskstorage.cassandra.thrift.CassandraThriftStoreManager", "cassandrathrift", "cassandra"),
    CASSANDRA_ASTYANAX("org.janusgraph.diskstorage.cassandra.astyanax.AstyanaxStoreManager", ImmutableList.of("cassandra", "astyanax"), "cassandra"),
    CASSANDRA_EMBEDDED("org.janusgraph.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager", "embeddedcassandra", "cassandra"),
    HBASE("org.janusgraph.diskstorage.hbase.HBaseStoreManager", "hbase", "hbase"),
    IN_MEMORY("org.janusgraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager", "inmemory", "inmemory");

    private final String managerClass;
    private final ImmutableList<String> shorthands;
    private final String backendName;

    StandardStoreManager(String managerClass, ImmutableList<String> shorthands, String backendName) {
        this.managerClass = managerClass;
        this.shorthands = shorthands;
        this.backendName = backendName;
    }

    StandardStoreManager(String managerClass, String shorthand, String backendName) {
        this(managerClass, ImmutableList.of(shorthand), backendName);
    }

    public List<String> getShorthands() {
        return shorthands;
    }

    public String getManagerClass() {
        return managerClass;
    }

    public String getBackEndName() {
        return backendName;
    }

    private static final ImmutableList<String> ALL_SHORTHANDS;
    private static final ImmutableMap<String, String> ALL_MANAGER_CLASSES;
    private static final ImmutableSet<String> ALL_BACKENDS;
    private static final ImmutableList<String> CASSANDRA_SHORTHANDS;
    private static final ImmutableList<String> HBASE_SHORTHANDS;
    private static final ImmutableList<String> BERKELEY_SHORTHANDS;

    static {
        StandardStoreManager backends[] = values();
        List<String> tempShorthands = new ArrayList<String>();
        List<String> tempBackends = new ArrayList<String>();
        List<String> tempCassandraShorthands = new ArrayList<String>();
        List<String> tempHbaseShorthands = new ArrayList<String>();
        List<String> tempBerkeleyShorthands = new ArrayList<String>();
        Map<String, String> tempClassMap = new HashMap<String, String>();
        for (int i = 0; i < backends.length; i++) {
            tempShorthands.addAll(backends[i].getShorthands());
            String backendName = backends[i].getBackEndName();
            tempBackends.add(backendName);
            for (String shorthand : backends[i].getShorthands()) {
                tempClassMap.put(shorthand, backends[i].getManagerClass());
                if (backendName.equals("cassandra")) tempCassandraShorthands.add(shorthand);
                if (backendName.equals("hbase")) tempHbaseShorthands.add(shorthand);
                if (backendName.equals("berkeley")) tempBerkeleyShorthands.add(shorthand);
            }
        }
        ALL_SHORTHANDS = ImmutableList.copyOf(tempShorthands);
        ALL_MANAGER_CLASSES = ImmutableMap.copyOf(tempClassMap);
        ALL_BACKENDS = ImmutableSet.copyOf(tempBackends);
        CASSANDRA_SHORTHANDS = ImmutableList.copyOf(tempCassandraShorthands);
        HBASE_SHORTHANDS = ImmutableList.copyOf(tempHbaseShorthands);
        BERKELEY_SHORTHANDS = ImmutableList.copyOf(tempBerkeleyShorthands);
    }

    public static List<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static Map<String, String> getAllManagerClasses() {
        return ALL_MANAGER_CLASSES;
    }

    public static List<String> getAllCassandraShorthands() {
        return CASSANDRA_SHORTHANDS;
    }

    public static List<String> getAllHbaseShorthands() {
        return HBASE_SHORTHANDS;
    }

    public static List<String> getAllBerkeleyShorthands() {
        return BERKELEY_SHORTHANDS;
    }
}

