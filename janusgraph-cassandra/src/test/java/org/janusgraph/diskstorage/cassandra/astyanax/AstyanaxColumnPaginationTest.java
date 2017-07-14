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

package org.janusgraph.diskstorage.cassandra.astyanax;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreTest;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.junit.Test;
import static org.junit.Assert.*;

public class AstyanaxColumnPaginationTest extends AbstractCassandraStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getAstyanaxConfiguration(getClass().getSimpleName());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration c) throws BackendException {
        return new AstyanaxStoreManager(c);
    }

    @Test
    public void ensureReadPageSizePropertySetCorrectly() {
        assertEquals(((AstyanaxStoreManager) manager).readPageSize, 4096);
    }

    @Test
    public void retrieveLessThanBoundaryColumnPagination() {
        Graph graph = JanusGraphFactory.open(getBaseStorageConfiguration());
        Vertex v = graph.addVertex();
        for(int i = 0; i < 4095; i++) {
            v.property(String.valueOf(i), i);
        }
        graph.tx().commit();

        assertEquals(4095, graph.traversal().V(v).valueMap().next().keySet().size());
    }

    @Test
    public void retrieveBoundaryColumnPagination() {
        Graph graph = JanusGraphFactory.open(getBaseStorageConfiguration());
        Vertex v = graph.addVertex();
        for(int i = 0; i < 4096; i++) {
            v.property(String.valueOf(i), i);
        }
        graph.tx().commit();

        assertEquals(4096, graph.traversal().V(v).valueMap().next().keySet().size());
    }

    @Test
    public void retrieveBeyondBoundaryColumnPagination() {
        Graph graph = JanusGraphFactory.open(getBaseStorageConfiguration());
        Vertex v = graph.addVertex();
        for(int i = 0; i < 4097; i++) {
            v.property(String.valueOf(i), i);
        }
        graph.tx().commit();

        assertEquals(4097, graph.traversal().V(v).valueMap().next().keySet().size());
    }

    @Test
    public void retrieveWayBeyondBoundaryColumnPagination() {
        Graph graph = JanusGraphFactory.open(getBaseStorageConfiguration());
        Vertex v = graph.addVertex();
        for (int i = 0; i < 20000; i++) {
            v.property(String.valueOf(i), i);
        }
        graph.tx().commit();

        assertEquals(20000, graph.traversal().V(v).valueMap().next().keySet().size());
    }
}
