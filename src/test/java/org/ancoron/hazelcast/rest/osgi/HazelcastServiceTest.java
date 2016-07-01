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


import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.mockito.Mockito.*;

/**
 *
 * @author ancoron
 */
public class HazelcastServiceTest {

    private InputStream stream(String resource) {
        return getClass().getClassLoader().getResourceAsStream(resource);
    }

    @Test
    public void writeSimple() throws Exception {
        Config cfg = new Config("HazelcastServiceTest");

        // disable multicast...
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        HazelcastService service = new HazelcastService();
        service.setHazelcast(hz);

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(request.getPathInfo()).thenReturn("/buckets/A/1");

        final InputStream testData = stream("test.json");
        when(request.getContentLength()).thenReturn(171);
        when(request.getContentType()).thenReturn("application/json");
        when(request.getInputStream()).thenReturn(new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return true;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(ReadListener readListener) {
            }

            @Override
            public int read() throws IOException {
                return testData.read();
            }
        });

        service.doPost(request, response);
    }
}
