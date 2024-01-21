/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.search.SearchHits;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;

@RunWith(RandomizedRunner.class)
public class SycamoreCluster extends OpenSearchIntegTestCase {

    Client client;

    final TestCluster cluster;

    @BeforeClass
    public static void beforeClass() {

    }

    @AfterClass
    public static void afterClass() {}

    public SycamoreCluster() throws Exception {
        // SUITE_SEED = randomLong();
        cluster = buildTestCluster(Scope.SUITE, 0L);
        currentCluster = cluster;
        // OpenSearchIntegTestCase.beforeClass();
        client = cluster.client();
    }

    public InternalTestCluster getCluster() {
        return (InternalTestCluster) cluster; //  OpenSearchIntegTestCase.internalCluster() ;
    }

    public void createIndex(String index, String mapping) {
        CreateIndexRequestBuilder bldr = client.admin().indices().prepareCreate(index);
        client.admin().indices().create(bldr.setMapping(mapping).request());
    }

    public void bulkLoad(String globalIndex, List<byte[]> docs) throws ExecutionException, InterruptedException {

        BulkRequestBuilder bldr = client.prepareBulk(globalIndex);
        for (byte[] doc : docs) {
            bldr.add(client.prepareIndex(globalIndex).setSource(doc, MediaTypeRegistry.JSON)
                .request());
        }
        BulkResponse res = client.bulk(bldr.request().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).get();

        for (BulkItemResponse r : res.getItems()) {
            System.out.println("Failure message: " + r.getFailureMessage());
        }
    }

    Gson gson = new GsonBuilder().create();

    public void bulkLoad(String globalIndex, String docs) throws ExecutionException, InterruptedException {


        /*
        BulkResponse res = client.prepareBulk(globalIndex)
        //for (byte[] doc : docs) {
        .add(client.prepareIndex().setSource(docs, MediaTypeRegistry.JSON).request()).get();
        //}
        // BulkResponse res = client.bulk(bldr.request()).get();

         */

        List<?> docList = gson.fromJson(docs, List.class);


        // String[] tokens = docs.split(",");
        List<byte[]> list = new ArrayList<>();
        for (Object doc : docList) {
            System.out.println("Document: " + doc);
            list.add(((String) doc).getBytes(StandardCharsets.UTF_8));
        }

        bulkLoad(globalIndex, list);

        Thread.sleep(100L);

        long docCount = 0;

        while (docCount < list.size()) {
            ClusterStatsResponse res = cluster.client().admin().cluster().clusterStats(new ClusterStatsRequest()).get();
            docCount = res.getIndicesStats().getDocs().getCount();
            System.out.println("Doc count: " + docCount);
            Thread.sleep(100L);
        }

        // ensureGreen(globalIndex);

        searchMatchAll(globalIndex);
    }

    //public void ensureGreen(String index) {
    //    super.ensureGreen(index);
    //}
    public SearchHits searchMatchAll(String index) throws ExecutionException, InterruptedException {

        getCurrentDocCount(index);

        SearchResponse res = client.prepareSearch(index).setQuery(matchAllQuery()).get();
        System.out.println("Total hits: " + res.getHits().getHits().length);
        return res.getHits();
    }

    private long getCurrentDocCount(String index) throws ExecutionException, InterruptedException {
        long docCount = -1;
        ClusterStatsResponse res = cluster.client().admin().cluster().clusterStats(new ClusterStatsRequest()).get();
        docCount = res.getIndicesStats().getDocs().getCount();
        System.out.println("Doc count: " + docCount);
        return docCount;
    }

    @Override
    protected Scope getCurrentClusterScope() {
        return Scope.SUITE;
    }
}
