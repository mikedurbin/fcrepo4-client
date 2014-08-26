/**
 * Copyright 2014 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.client.impl;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

import static com.hp.hpl.jena.rdf.model.ResourceFactory.createProperty;

import static org.fcrepo.kernel.RdfLexicon.HAS_CONTENT;
import static org.fcrepo.kernel.RdfLexicon.HAS_ORIGINAL_NAME;
import static org.fcrepo.kernel.RdfLexicon.HAS_MIME_TYPE;
import static org.fcrepo.kernel.RdfLexicon.HAS_SIZE;
import static org.fcrepo.kernel.RdfLexicon.RESTAPI_NAMESPACE;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Property;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;

import org.apache.jena.atlas.lib.NotImplemented;

import org.fcrepo.client.FedoraContent;
import org.fcrepo.client.FedoraDatastream;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraObject;
import org.fcrepo.client.FedoraRepository;
import org.fcrepo.client.utils.HttpHelper;

import org.slf4j.Logger;

/**
 * A Fedora Datastream Impl.
 *
 * @author escowles
 * @since 2014-08-25
 */
public class FedoraDatastreamImpl extends FedoraResourceImpl implements FedoraDatastream {
    private static final Logger LOGGER = getLogger(FedoraDatastreamImpl.class);
    protected static final Property REST_API_DIGEST = createProperty(RESTAPI_NAMESPACE + "digest");
    private boolean hasContent;
    private Node contentSubject;

    /**
     * Constructor for FedoraDatastreamImpl
     *
     * @param repository Repository that created this object.
     * @param httpHelper HTTP helper for making repository requests
     * @param path Path of the datastream in the repository
     */
    public FedoraDatastreamImpl(final FedoraRepository repository, final HttpHelper httpHelper, final String path) {
        super(repository, httpHelper, path);
        contentSubject = NodeFactory.createURI( repository.getRepositoryUrl() + path + "/fcr:content" );
    }

    @Override
    public void setGraph( final Graph graph ) {
        super.setGraph( graph );
        hasContent = getTriple( subject, HAS_CONTENT ) != null;
    }

    @Override
    public boolean hasContent() throws FedoraException {
        return hasContent;
    }

    @Override
    public FedoraObject getObject() throws FedoraException {
        return repository.getObject( path.substring(0, path.lastIndexOf("/")) );
    }

    @Override
    public URI getContentDigest() throws FedoraException {
        final Node contentDigest = getObjectValue( REST_API_DIGEST );
        try {
            if ( contentDigest == null ) {
                return null;
            }

            return new URI( contentDigest.getURI() );
        } catch ( URISyntaxException e ) {
            throw new FedoraException("Error parsing checksum URI: " + contentDigest.getURI(), e);
        }
    }

    @Override
    public Long getContentSize() throws FedoraException {
        final Node size = getObjectValue( HAS_SIZE );
        if ( size == null ) {
            return null;
        }

        return new Long( size.getLiteralValue().toString() );
    }

    @Override
    public String getFilename() throws FedoraException {
        final Node filename = getObjectValue( HAS_ORIGINAL_NAME );
        if ( filename == null ) {
            return null;
        }

        return filename.getLiteralValue().toString();
    }

    @Override
    public String getContentType() throws FedoraException {
        final Node contentType = getObjectValue( HAS_MIME_TYPE );
        if ( contentType == null ) {
            return null;
        }

        return contentType.getLiteralValue().toString();
    }

    @Override
    public void updateContent( final FedoraContent content ) throws FedoraException {
        final HttpPut put = httpHelper.createContentPutMethod( path, null, content );

        try {
            final HttpResponse response = httpHelper.execute( put );
            final StatusLine status = response.getStatusLine();
            final String uri = put.getURI().toString();

            if ( status.getStatusCode() == CREATED.getStatusCode()
                    || status.getStatusCode() == NO_CONTENT.getStatusCode()) {
                LOGGER.debug("content updated successfully for resource {}", uri);
            } else if ( status.getStatusCode() == FORBIDDEN.getStatusCode()) {
                LOGGER.error("request for resource {} is not authorized.", uri);
                throw new ForbiddenException("request for resource " + uri + " is not authorized.");
            } else if ( status.getStatusCode() == NOT_FOUND.getStatusCode()) {
                LOGGER.error("resource {} does not exist, cannot retrieve", uri);
                throw new NotFoundException("resource " + uri + " does not exist, cannot retrieve");
            } else if ( status.getStatusCode() == CONFLICT.getStatusCode()) {
                LOGGER.error("checksum mismatch for {}", uri);
                throw new FedoraException("checksum mismatch for resource " + uri);
            } else {
                LOGGER.error("error retrieving resource {}: {} {}", uri, status.getStatusCode(),
                             status.getReasonPhrase());
                throw new FedoraException("error retrieving resource " + uri + ": " + status.getStatusCode() + " " +
                                          status.getReasonPhrase());
            }

            // update properties from server
            httpHelper.loadProperties(this);

        } catch (final FedoraException e) {
            throw e;
        } catch (final Exception e) {
            LOGGER.error("could not encode URI parameter", e);
            throw new FedoraException(e);
        } finally {
            put.releaseConnection();
        }
    }

    @Override
    public InputStream getContent() throws FedoraException {
        final HttpGet get = httpHelper.createGetMethod( path + "/fcr:content", null );
        final String uri = get.getURI().toString();

        try {
            final HttpResponse response = httpHelper.execute( get );
            final StatusLine status = response.getStatusLine();

            if ( status.getStatusCode() == OK.getStatusCode()) {
                return response.getEntity().getContent();
            } else if ( status.getStatusCode() == FORBIDDEN.getStatusCode()) {
                LOGGER.error("request for resource {} is not authorized.", uri);
                throw new ForbiddenException("request for resource " + uri + " is not authorized.");
            } else if ( status.getStatusCode() == NOT_FOUND.getStatusCode()) {
                LOGGER.error("resource {} does not exist, cannot retrieve", uri);
                throw new NotFoundException("resource " + uri + " does not exist, cannot retrieve");
            } else {
                LOGGER.error("error retrieving resource {}: {} {}", uri, status.getStatusCode(),
                             status.getReasonPhrase());
                throw new FedoraException("error retrieving resource " + uri + ": " + status.getStatusCode() + " " +
                                          status.getReasonPhrase());
            }
        } catch (final Exception e) {
            LOGGER.error("could not encode URI parameter", e);
            throw new FedoraException(e);
        } finally {
            get.releaseConnection();
        }
    }

    @Override
    public void checkFixity() {
        throw new NotImplemented("Method checkFixity() is not implemented");
    }

    private Node getObjectValue( final Property property ) {
        if ( !hasContent ) {
            return null;
        }

        final Triple t = getTriple( contentSubject, property );
        if ( t == null ) {
            return null;
        }

        return t.getObject();
    }
}
