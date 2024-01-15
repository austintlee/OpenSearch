/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

//import com.carrotsearch.randomizedtesting.RandomizedRunner;
//import org.junit.runner.RunWith;

//@RunWith(RandomizedRunner.class)
public class SycamoreCluster extends OpenSearchIntegTestCase {

    final TestCluster cluster;
    public SycamoreCluster() throws Exception {
        // SUITE_SEED = randomLong();
        cluster = buildTestCluster(Scope.SUITE, 0L);
        // OpenSearchIntegTestCase.beforeClass();
    }

    public InternalTestCluster getCluster() {
        return (InternalTestCluster) cluster; //  OpenSearchIntegTestCase.internalCluster() ;
    }

    @Override
    protected Scope getCurrentClusterScope() {
        return Scope.SUITE;
    }
}
