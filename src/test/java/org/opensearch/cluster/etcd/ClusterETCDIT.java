/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusterETCDIT extends OpenSearchRestTestCase {

    private static final String CLUSTER_ETCD_PLUGIN_NAME = "cluster-etcd";

    public void testClusterEtcdPluginInstalled() throws IOException, ParseException {
        final Request request = new Request(RestRequest.Method.GET.name(), String.join("/", "_cat", "plugins"));
        request.addParameter("local", "true");
        final Response response = client().performRequest(request);
        assertOK(response);

        final String responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertNotNull(responseBody);
        Assert.assertTrue(responseBody.contains(CLUSTER_ETCD_PLUGIN_NAME));
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Block deleting things, since that usually requires a cluster manager.
        return true;
    }
}
