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
package org.ancoron.hazelcast.rest.servlet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.stream.JsonWriter;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;

import static javax.servlet.http.HttpServletResponse.*;

/**
 *
 * @author ancoron
 */
public class HazelcastMapServlet extends HttpServlet
        implements DistributedObjectListener
{

    private static final long serialVersionUID = -7084487112704157155L;

    private static final Logger LOG = Logger.getLogger(HazelcastMapServlet.class
            .getName());

    protected static final int T_APPLICATION_OCTETSTREAM = 0;
    protected static final int T_IMAGE_GIF = 8;
    protected static final int T_IMAGE_PNG = 7;
    protected static final int T_IMAGE_JPEG = 6;
    protected static final int T_TEXT_XML = 5;
    protected static final int T_APPLICATION_XML = 4;
    protected static final int T_TEXT_HTML = 3;
    protected static final int T_TEXT_PLAIN = 2;
    protected static final int T_APPLICATION_JSON = 1;

    protected static final String TYPE_APPLICATION_OCTETSTREAM = "application/octet-stream";
    protected static final String TYPE_IMAGE_GIF = "image/gif";
    protected static final String TYPE_IMAGE_PNG = "image/png";
    protected static final String TYPE_IMAGE_JPEG = "image/jpeg";
    protected static final String TYPE_TEXT_XML = "text/xml";
    protected static final String TYPE_APPLICATION_XML = "application/xml";
    protected static final String TYPE_TEXT_HTML = "text/html";
    protected static final String TYPE_TEXT_PLAIN = "text/plain";
    protected static final String TYPE_APPLICATION_JSON = "application/json";

    private static final Pattern ROUTE_BUCKET = Pattern.compile("^/([^/]+)$");
    private static final Pattern ROUTE_KEY = Pattern.compile(
            "^/([^/]+)/([^/]+)$");

    protected HazelcastInstance hazelcast;
    protected final ConcurrentMap<String, Long> bucketCreation
            = new ConcurrentHashMap<>();

    public HazelcastInstance getHazelcast()
    {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast)
    {
        this.hazelcast = hazelcast;
        if (hazelcast != null) {
            hazelcast.addDistributedObjectListener(this);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        final String path = req.getPathInfo();
        Matcher matcher = ROUTE_KEY.matcher(path);

        if (matcher.matches()) {
            final String map = matcher.group(1);
            final String key = matcher.group(2);
            if (getValue(map, key) == null) {
                doPut(req, resp);
            } else {
                resp.sendError(SC_CONFLICT,
                        "Key already exists in bucket '" + map + "': " + key);
            }
        } else if ((matcher = ROUTE_BUCKET.matcher(path)).matches()) {
            final String map = matcher.group(1);
            String ttl_str = req.getParameter("ttl");
            String backups_str = req.getParameter("backups");
            String maxdata_str = req.getParameter("maxData");
            try {
                final int ttl;
                if (ttl_str != null) {
                    // no negative TTL, please
                    ttl = Math.max(0, Integer.valueOf(ttl_str));
                } else {
                    ttl = 300; // 5 minutes per default
                }

                final int backups;
                if (backups_str != null) {
                    // no negative backups, please
                    backups = Math.max(0, Integer.valueOf(backups_str));
                } else {
                    backups = 0; // no backup by default
                }

                final int maxdata;
                if (maxdata_str != null) {
                    // no negative size, please
                    maxdata = Math.max(0, Integer.valueOf(maxdata_str));
                } else {
                    maxdata = 128; // 128 MiB of data by default
                }

                createBucket(map, ttl, backups, maxdata);

                resp.setStatus(SC_NO_CONTENT);
            } catch (FileAlreadyExistsException x) {
                sendError(resp, SC_CONFLICT, "Bucket already exists: " + map, x);
            } catch (NumberFormatException x) {
                sendError(resp, SC_BAD_REQUEST, "Invalid parameter value specified", x);
            }
        } else {
            sendError(resp, SC_NOT_FOUND, "Resource not found: " + path);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        final String path = req.getPathInfo();
        Matcher matcher = ROUTE_KEY.matcher(path);

        if (matcher.matches()) {
            String map = matcher.group(1);
            String key = matcher.group(2);

            try {
                byte[] bytes = getValue(map, key);
                if (bytes == null) {
                    sendError(resp, SC_NOT_FOUND,
                            "Key does not exist in bucket '" + map + "': " + key);
                } else if (bytes.length == 1) {
                    resp.setContentType(mapContentType(bytes[0]));
                    resp.setContentLength(0);
                    resp.setStatus(SC_NO_CONTENT);
                } else {
                    resp.setStatus(SC_OK);
                    resp.setContentType(mapContentType(bytes[0]));
                    resp.setContentLength(bytes.length - 1);
                    try (final ServletOutputStream out = resp.getOutputStream()) {
                        out.write(bytes, 1, bytes.length - 1);
                    }
                }
            } catch (FileNotFoundException x) {
                sendError(resp, SC_NOT_FOUND, "Bucket not found: " + map, x);
            }
        } else if ((matcher = ROUTE_BUCKET.matcher(path)).matches()) {
            String map = matcher.group(1);
            try {
                verifyBucket(map);
                IMap<String, byte[]> m = hazelcast.getMap(map);
                MapConfig config = hazelcast.getConfig().getMapConfig(map);
                resp.setStatus(SC_OK);
                try (JsonWriter writer = new JsonWriter(resp.getWriter())) {
                    writer.beginObject();
                    writer.name("count").value(m.size());
                    writer.name("ttl").value(config.getTimeToLiveSeconds());
                    writer.name("backups").value(config.getBackupCount());
                    writer.name("maxData").value(config.getMaxSizeConfig().getSize());
                    writer.endObject();
                }
            } catch (FileNotFoundException x) {
                sendError(resp, SC_NOT_FOUND, "Bucket not found: " + map, x);
            }
        } else {
            sendError(resp, SC_NOT_FOUND, "Resource not found: " + path);
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        final String path = req.getPathInfo();
        Matcher matcher = ROUTE_KEY.matcher(path);

        if (matcher.matches()) {
            final String map = matcher.group(1);
            final String key = matcher.group(2);
            String contentType = req.getContentType();
            if (contentType == null) {
                resp.sendError(SC_UNSUPPORTED_MEDIA_TYPE,
                        "No content type specified");
                return;
            }
            int length = req.getContentLength();
            if (length < 0) {
                resp.sendError(SC_LENGTH_REQUIRED,
                        "No content length specified");
                return;
            }

            try (final InputStream in = req.getInputStream()) {
                setValue(map, key, contentType, length, in);
                resp.setStatus(SC_NO_CONTENT);
            } catch (IllegalArgumentException x) {
                resp.sendError(SC_UNSUPPORTED_MEDIA_TYPE,
                        "Content-Type not supported: " + contentType);
            } catch (FileNotFoundException x) {
                resp.sendError(SC_NOT_FOUND, x.getMessage());
            } catch (IOException x) {
                log("Unable to set value for bucket '" + map + "' and key '" + key + "'",
                        x);
                resp.sendError(SC_INTERNAL_SERVER_ERROR,
                        x.getMessage());
            }
        } else if ((matcher = ROUTE_BUCKET.matcher(path)).matches()) {
            final String map = matcher.group(1);
            resp.sendError(SC_METHOD_NOT_ALLOWED);
        } else {
            resp.sendError(SC_NOT_FOUND);
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        final String path = req.getPathInfo();
        Matcher matcher = ROUTE_KEY.matcher(path);

        try {
            if (matcher.matches()) {
                final String map = matcher.group(1);
                final String key = matcher.group(2);

                verifyBucket(map);

                deleteValue(map, key);
            } else if ((matcher = ROUTE_BUCKET.matcher(path)).matches()) {
                final String map = matcher.group(1);
                deleteBucket(map);
                resp.sendError(SC_METHOD_NOT_ALLOWED);
            } else {
                sendError(resp, SC_NOT_FOUND, "Resource not found: " + path);
            }
        } catch (FileNotFoundException x) {
            sendError(resp, SC_NOT_FOUND, x.getMessage());
        }
    }

    public void createBucket(String map, int ttl, int backups, int mib)
            throws IOException
    {
        if (bucketCreation.containsKey(map)) {
            throw new FileAlreadyExistsException(null, null,
                    "Bucket already exists: " + map);
        }

        Map<String, MapConfig> mapConfigs = hazelcast.getConfig()
                .getMapConfigs();
        MapConfig config = new MapConfig(map);
        config.setTimeToLiveSeconds(ttl);
        config.setEvictionPolicy(EvictionPolicy.LRU);
        config.setInMemoryFormat(InMemoryFormat.BINARY);
        config.setBackupCount(backups);

        int nodes = hazelcast.getCluster().getMembers().size();
        MaxSizeConfig max = new MaxSizeConfig(mib / nodes,
                MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE);
        config.setMaxSizeConfig(max);
        mapConfigs.put(map, config);

        // pre-fill local map configuration timestamp...
        bucketCreation.putIfAbsent(map, System.currentTimeMillis());

        // this should always be the first call to the map...
        hazelcast.getMap(map);
    }

    public void deleteBucket(String map) throws IOException
    {
        verifyBucket(map);

        hazelcast.getConfig().getMapConfigs().remove(map);
        hazelcast.getDistributedObject(MapService.SERVICE_NAME, map).destroy();
        bucketCreation.remove(map);
    }

    public void deleteValue(String map, String key) throws IOException
    {
        verifyBucket(map);

        if (getValue(map, key) != null) {
            hazelcast.getMap(map).delete(key);
        } else {
            throw new FileNotFoundException(
                    "Key not found in bucket '" + map + "': " + key);
        }
    }

    public void setValue(String map, String key, String contentType,
            int length, final InputStream in) throws IOException
    {
        verifyBucket(map);

        byte type = mapContentType(contentType);
        IMap<String, byte[]> m = hazelcast.getMap(map);
        int offset = 1;
        byte[] data = new byte[length + offset];
        data[0] = type;
        int num;
        while ((num = in.read(data, offset, length)) > 0) {
            offset += num;
            if (offset >= data.length) {
                break;
            }
        }

        m.set(key, data);
    }

    protected void verifyBucket(String map) throws FileNotFoundException
    {
        if (!bucketCreation.containsKey(map)) {
            throw new FileNotFoundException("Bucket not found: " + map);
        }
    }

    public byte[] getValue(String map, String key) throws IOException
    {
        verifyBucket(map);

        IMap<String, byte[]> m = hazelcast.getMap(map);
        return m.get(key);
    }

    public void sendError(HttpServletResponse resp, int status, String message)
            throws IOException
    {
        sendError(resp, status, message, null);
    }

    public void sendError(HttpServletResponse resp, int status, String message,
            Throwable error) throws IOException
    {
        if (error != null) {
            LOG.log(Level.WARNING, message, error);
        } else {
            LOG.log(Level.WARNING, message);
        }

        resp.setStatus(status);
        resp.setContentType(TYPE_APPLICATION_JSON);

        try (JsonWriter writer = new JsonWriter(resp.getWriter())) {
            writer.beginObject();
            writer.name("error").value(message);
            writer.endObject();
        }
    }

    protected byte mapContentType(final String contentType)
    {
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
        throw new IllegalArgumentException(
                "Content-Type not supported: " + contentType);
    }

    protected String mapContentType(final byte contentType)
    {
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
        throw new IllegalArgumentException(
                "Content-Type not supported: " + contentType);
    }

    @Override
    public void distributedObjectCreated(DistributedObjectEvent event)
    {
        if (DistributedObjectEvent.EventType.CREATED == event.getEventType()) {
            String serviceName = event.getServiceName();
            if (MapService.SERVICE_NAME.equals(serviceName)) {
                bucketCreation.putIfAbsent(
                        String.class.cast(event.getObjectName()),
                        System.currentTimeMillis()
                );
            }
        }
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent event)
    {
        if (DistributedObjectEvent.EventType.DESTROYED == event.getEventType()) {
            String serviceName = event.getServiceName();
            if (MapService.SERVICE_NAME.equals(serviceName)) {
                bucketCreation.remove(
                        String.class.cast(event.getObjectName())
                );
            }
        }
    }
}
