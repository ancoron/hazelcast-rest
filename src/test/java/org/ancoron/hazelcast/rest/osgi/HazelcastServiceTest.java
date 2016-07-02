/*
 * Copyright 2016 ancoron.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ancoron.hazelcast.rest.osgi;


import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import org.ancoron.hazelcast.rest.servlet.HazelcastMapServlet;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;


/**
 *
 * @author ancoron
 */
public class HazelcastServiceTest {

    private InputStream stream(String resource) {
        return getClass().getClassLoader().getResourceAsStream(resource);
    }

    @Test
    public void lifecycleSimple() throws Exception {
        Config cfg = new Config("HazelcastServiceTest");

        // disable multicast...
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        HazelcastMapServlet service = new HazelcastMapServlet();
        service.setHazelcast(hz);

        // Step #1: create a bucket
        service.createBucket("A", 60, 0, 128);

        // Step #2: insert some data for a key
        MessageDigest md5 = DigestUtils.getMd5Digest();
        MessageDigest sha1 = DigestUtils.getSha1Digest();
        try (InputStream testData = new DigestInputStream(
                new DigestInputStream(stream("test.json"), sha1), md5))
        {
            service.setValue("A", "1", "application/json", 171, testData);
        }

        // Step #3: fetch some data for a key
        byte[] value = service.getValue("A", "1");
        byte[] data = new byte[value.length - 1];
        System.arraycopy(value, 1, data, 0, data.length);

        Assert.assertThat(DigestUtils.md5Hex(data),
                CoreMatchers.is(Hex.encodeHexString(md5.digest()))
        );
        Assert.assertThat(DigestUtils.sha1Hex(data),
                CoreMatchers.is(Hex.encodeHexString(sha1.digest()))
        );

        // Step #4: delete the bucket
        service.deleteBucket("A");
    }
}
