package com.ganesha.ingest;

import com.ganesha.ingest.page.Page;
import com.ganesha.ingest.schema.ParsingSchema;
import com.ganesha.ingest.sources.Source;
import com.ganesha.ingest.sources.Sources;
import lombok.extern.log4j.Log4j2;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.hc.client5.http.config.RequestConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Log4j2
@Component
public class Reader {

    public static final String SCHEMA = "schema";
    private static final int CONNECTION_TIMEOUT = 5000; // 5 seconds
    private static final int SOCKET_TIMEOUT = 10000;    // 10 seconds
    private static final int MAX_CONNECTIONS = 100;

    @Autowired
    private Sources sources;

    private final Map<String, ParsingSchema> schemaByTypeServices;
    private final CloseableHttpClient httpClient;

    @Autowired
    public Reader(List<ParsingSchema> schemaTypeServices) {
        this.schemaByTypeServices = schemaTypeServices.stream()
                .collect(Collectors.toMap(ParsingSchema::getSchemaId, Function.identity()));
        
        // Configure connection pool
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(MAX_CONNECTIONS);
        connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS);

        // Create request config with timeouts
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(CONNECTION_TIMEOUT))
                .setResponseTimeout(Timeout.ofMilliseconds(SOCKET_TIMEOUT))
                .build();

        // Create HTTP client with timeouts and connection pool
        this.httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    public Optional<Page> read(String baseUrl, String readUrl, int level) throws IOException {
        log.debug("Attempting to read URL: {} with level: {}", readUrl, level);
        
        final AtomicReference<Optional<Page>> result = new AtomicReference<>(Optional.empty());
        final AtomicReference<IOException> exceptionRef = new AtomicReference<>();

        try {
            ClassicHttpRequest httpGet = ClassicRequestBuilder.get(readUrl)
                    .build();
            
            httpClient.execute(httpGet, response -> {
                try {
                    int statusCode = response.getCode();
                    String reasonPhrase = response.getReasonPhrase();
                    log.info("Response from url {} response code {} reason {} level {}", 
                            readUrl, statusCode, reasonPhrase, level);

                    if (statusCode >= 400) {
                        throw new IOException("HTTP request failed with status " + statusCode + ": " + reasonPhrase);
                    }

                    final HttpEntity entity = response.getEntity();
                    if (entity == null) {
                        throw new IOException("No response entity received from " + readUrl);
                    }

                    String body = EntityUtils.toString(entity);
                    Optional<ParsingSchema> schema = allocateParsingSchema(baseUrl, level);

                    if (schema.isEmpty()) {
                        log.warn("There is no suitable schema for url {}", baseUrl);
                        return null;
                    }

                    Page page = schema.get().convert(body, level, readUrl);
                    result.set(Optional.of(page));
                    
                } catch (Exception e) {
                    log.error("Error processing response from url: {} level: {}", readUrl, level, e);
                    if (e instanceof IOException) {
                        exceptionRef.set((IOException) e);
                    } else {
                        exceptionRef.set(new IOException("Error processing response", e));
                    }
                } finally {
                    response.close();
                }
                return null;
            });

            if (exceptionRef.get() != null) {
                throw exceptionRef.get();
            }

        } catch (IOException e) {
            log.error("Failed to execute HTTP request for url: {} level: {}", readUrl, level, e);
            throw e;
        }

        return result.get();
    }

    private Optional<ParsingSchema> allocateParsingSchema(String url, int level) {
        Source source = sources.getSource(url);
        Map<String, String> sourceLevelProps = source.getLevel(level);
        String schemaType = sourceLevelProps.get(SCHEMA);
        ParsingSchema schema = schemaByTypeServices.get(schemaType);
        
        if (schema == null) {
            log.warn("No schema found for type: {} at level: {}", schemaType, level);
            return Optional.empty();
        }
        
        schema.setParameters(sourceLevelProps);
        return Optional.of(schema);
    }
}
