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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.common.DistributedStoreManager.Deployment;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.testcategory.OrderedKeyStoreTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public abstract class DistributedStoreManagerTest<T extends DistributedStoreManager> {

    protected T manager;
    protected KeyColumnValueStore store;
    
    public Deployment getDeploymentType() {
        return Deployment.LOCAL;
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGetDeployment() {
        assertEquals(getDeploymentType(), manager.getDeployment());
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGetLocalKeyPartition() throws BackendException {
        assumeTrue(manager.getFeatures().hasLocalKeyPartition());
        List<KeyRange> local = manager.getLocalKeyPartition();
        assertNotNull(local);

        if (getDeploymentType() == Deployment.LOCAL) {
            assertEquals(1, local.size());
            assertNotNull(local.get(0).getStart());
            assertNotNull(local.get(0).getEnd());
        } else {
            assertEquals(0, local.size());
        }
    }
}
