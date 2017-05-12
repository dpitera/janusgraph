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

package org.janusgraph.diskstorage.common;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_ROOT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.diskstorage.configuration.BasicConfiguration.Restriction.NONE;


import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocalStoreManagerTest {

    public class LocalStoreManagerSampleImplementation extends LocalStoreManager {
        public LocalStoreManagerSampleImplementation(Configuration c) throws BackendException {
            super(c);
        }

        /*
         * The following methods are placeholders to adhere to the StoreManager interface.
         */
        @Override
        public List<KeyRange> getLocalKeyPartition() throws BackendException {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public StoreFeatures getFeatures() {
            return null;
        }

        @Override
        public void clearStorage() throws BackendException {}

        @Override
        public void close() throws BackendException {}

        @Override
        public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
            return null;
        }
    }

    public Map<ConfigOption, String> getBaseConfigurationMap() {
        Map<ConfigOption, String> map = new HashMap<ConfigOption, String>();
        map.put(STORAGE_BACKEND, "berkeleyje");
        return map;
    }

    public LocalStoreManager getStoreManager(Map<ConfigOption, String> map) throws BackendException {
        ModifiableConfiguration mc = new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration(new BaseConfiguration()), NONE);
        map.forEach((key, value) -> {
            mc.set(key, value);
        });
        return new LocalStoreManagerSampleImplementation(mc);
    }

    @Test
    public void directoryShouldEqualSuppliedDirectory() throws BackendException {
        Map<ConfigOption, String> map = getBaseConfigurationMap();
        map.put(STORAGE_DIRECTORY, "specific/absolute/directory");
        LocalStoreManager mgr = getStoreManager(map);
        assertEquals("specific/absolute/directory", mgr.directory.getPath());
    }

    @Test
    public void directoryShouldEqualStorageRootPlusGraphName() throws BackendException {
        Map<ConfigOption, String> map = getBaseConfigurationMap();
        map.put(STORAGE_ROOT, "temp/root");
        map.put(GRAPH_NAME, "randomGraphName");
        LocalStoreManager mgr = getStoreManager(map);
        assertEquals("temp/root/randomGraphName", mgr.directory.getPath());
    }

    @Test
    public void shouldThrowError() {
        boolean errorFound = false;
        Map<ConfigOption, String> map = getBaseConfigurationMap();
        map.put(GRAPH_NAME, "randomGraphName");
        try{
            LocalStoreManager mgr = getStoreManager(map);
        } catch (Exception e) {
            errorFound = true;
        }
        assertTrue(errorFound);
    }
}

