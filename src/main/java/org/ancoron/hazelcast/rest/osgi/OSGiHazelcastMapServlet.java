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

import org.ancoron.hazelcast.rest.servlet.HazelcastMapServlet;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.http.HttpService;
import org.osgi.service.http.NamespaceException;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 *
 * @author ancoron
 */
@Component
public class OSGiHazelcastMapServlet extends HazelcastMapServlet {

    private static final long serialVersionUID = -5777719125761095877L;

    private static final Logger LOG = Logger.getLogger(OSGiHazelcastMapServlet.class.getName());

    private HttpService http;

    public HttpService getHttp() {
        return http;
    }

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    public void setHttp(HttpService http) {
        this.http = http;
    }

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    @Override
    public void setHazelcast(HazelcastInstance hazelcast) {
        super.setHazelcast(hazelcast);
    }

    @Activate
    public void configure(Map<String, Object> config)
            throws ServletException, NamespaceException
    {
        try {
            http.unregister("/buckets");
        } catch (IllegalArgumentException x) {}
        http.registerServlet("/buckets", this, null, http.createDefaultHttpContext());
    }

    @Deactivate
    public void unconfigure() {
        http.unregister("/buckets");
    }
}
