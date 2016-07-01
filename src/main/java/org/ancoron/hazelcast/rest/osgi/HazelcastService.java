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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class HazelcastService extends HttpServlet {

    private static final long serialVersionUID = -5777719125761095877L;

    private HazelcastInstance hazelcast;
    private HttpService http;

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public HttpService getHttp() {
        return http;
    }

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    public void setHttp(HttpService http) {
        this.http = http;
    }

    @Activate
    @Modified
    public void configure(Map<String, Object> config)
            throws ServletException, NamespaceException
    {
        http.registerServlet("/hazelcast", this, null, http.createDefaultHttpContext());
    }

    @Deactivate
    public void unconfigure() {
        http.unregister("/hazelcast");
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        super.doDelete(req, resp);

        
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        super.doPut(req, resp); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String path = req.getPathInfo();

        if (path.startsWith("/buckets/")) {
            int index = path.indexOf("/", 9);
            String map = path.substring(9, index);
            if (index >= path.length()) {
                resp.sendError(404, "No key specified");
            }
            String key = path.substring(index + 1);

            IMap<String, byte[]> m = hazelcast.getMap(map);

            String contentType = req.getContentType();
            byte type = mapContentType(contentType);
            int length = req.getContentLength();
            if (length < 0) {
                resp.sendError(400, "No content length specified");
            }

            int offset = 1;
            byte[] data = new byte[length + offset];
            data[0] = type;

            try (ServletInputStream in = req.getInputStream()) {
                int num;
                while ((num = in.read(data, offset, length)) > 0) {
                    offset += num;
                    if (offset >= data.length) {
                        break;
                    }
                }
            }

            m.set(key, data);
            resp.setStatus(204);
        } else {
            resp.sendError(400);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        String path = req.getPathInfo();

        if (path.startsWith("/buckets/")) {
            int index = path.indexOf("/", 9);
            String map = path.substring(9, index);
            if (index >= path.length()) {
                resp.sendError(404, "No key specified");
            }
            String key = path.substring(index + 1);

            byte[] bytes = (byte[]) hazelcast.getMap(map).get(key);
            if (bytes == null) {
                resp.setContentLength(0);
                resp.setStatus(204);
            } else if (bytes.length == 1) {
                resp.setContentType(mapContentType(bytes[0]));
                resp.setContentLength(0);
                resp.setStatus(200);
            } else {
                resp.setStatus(200);
                resp.setContentType(mapContentType(bytes[0]));
                resp.setContentLength(bytes.length - 1);
                try (ServletOutputStream out = resp.getOutputStream()) {
                    out.write(bytes, 1, bytes.length - 1);
                }
            }
        } else {
            resp.sendError(404);
        }
    }

    private byte mapContentType(final String contentType) {
        if (contentType == null) {
            throw new IllegalArgumentException("No Content-Type specified");
        }

        switch (contentType) {
            case TYPE_APPLICATION_JSON:
                return T_APPLICATION_JSON;
            case TYPE_TEXT_PLAIN:
                return T_TEXT_PLAIN;
            case TYPE_TEXT_HTML:
                return T_TEXT_HTML;
            case TYPE_APPLICATION_XML:
                return T_APPLICATION_XML;
            case TYPE_TEXT_XML:
                return T_TEXT_XML;
            case TYPE_IMAGE_JPEG:
                return T_IMAGE_JPEG;
            case TYPE_IMAGE_PNG:
                return T_IMAGE_PNG;
            case TYPE_IMAGE_GIF:
                return T_IMAGE_GIF;
            case TYPE_APPLICATION_OCTETSTREAM:
                return T_APPLICATION_OCTETSTREAM;
        }

        throw new IllegalArgumentException("Content-Type not supported: " + contentType);
    }

    private String mapContentType(final byte contentType) {
        switch (contentType) {
            case T_APPLICATION_JSON:
                return TYPE_APPLICATION_JSON;
            case T_TEXT_PLAIN:
                return TYPE_TEXT_PLAIN;
            case T_TEXT_HTML:
                return TYPE_TEXT_HTML;
            case T_APPLICATION_XML:
                return TYPE_APPLICATION_XML;
            case T_TEXT_XML:
                return TYPE_TEXT_XML;
            case T_IMAGE_JPEG:
                return TYPE_IMAGE_JPEG;
            case T_IMAGE_PNG:
                return TYPE_IMAGE_PNG;
            case T_IMAGE_GIF:
                return TYPE_IMAGE_GIF;
            case T_APPLICATION_OCTETSTREAM:
                return TYPE_APPLICATION_OCTETSTREAM;
        }

        throw new IllegalArgumentException("Content-Type not supported: " + contentType);
    }

    private static final int T_APPLICATION_OCTETSTREAM = 0;
    private static final int T_IMAGE_GIF = 8;
    private static final int T_IMAGE_PNG = 7;
    private static final int T_IMAGE_JPEG = 6;
    private static final int T_TEXT_XML = 5;
    private static final int T_APPLICATION_XML = 4;
    private static final int T_TEXT_HTML = 3;
    private static final int T_TEXT_PLAIN = 2;
    private static final int T_APPLICATION_JSON = 1;
    private static final String TYPE_APPLICATION_OCTETSTREAM = "application/octet-stream";
    private static final String TYPE_IMAGE_GIF = "image/gif";
    private static final String TYPE_IMAGE_PNG = "image/png";
    private static final String TYPE_IMAGE_JPEG = "image/jpeg";
    private static final String TYPE_TEXT_XML = "text/xml";
    private static final String TYPE_APPLICATION_XML = "application/xml";
    private static final String TYPE_TEXT_HTML = "text/html";
    private static final String TYPE_TEXT_PLAIN = "text/plain";
    private static final String TYPE_APPLICATION_JSON = "application/json";
}
