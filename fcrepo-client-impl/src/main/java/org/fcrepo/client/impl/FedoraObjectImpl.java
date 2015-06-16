/**
 * Copyright 2015 DuraSpace, Inc.
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

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.fcrepo.kernel.RdfLexicon.CONTAINS;
import static org.fcrepo.kernel.RdfLexicon.HAS_MIXIN_TYPE;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraObject;
import org.fcrepo.client.FedoraRepository;
import org.fcrepo.client.FedoraResource;
import org.fcrepo.client.ForbiddenException;
import org.fcrepo.client.utils.HttpHelper;
import org.slf4j.Logger;

/**
 * A Fedora Object Impl.
 *
 * @author lsitu
 * @author escowles
 * @since 2014-08-11
 */
public class FedoraObjectImpl extends FedoraResourceImpl implements FedoraObject {
    private static final Logger LOGGER = getLogger(FedoraObjectImpl.class);
    private final static Node binaryType = NodeFactory.createLiteral("fedora:binary");

    /**
     * Constructor for FedoraObjectImpl
     *
     * @param repository FedoraRepository that created this object
     * @param httpHelper HTTP helper for making repository requests
     * @param path Repository path
     */
    public FedoraObjectImpl(final FedoraRepository repository, final HttpHelper httpHelper, final String path) {
        super(repository, httpHelper, path);
    }

    /**
     * Get the Object and Datastream nodes that are children of the current Object.
     *
     * @param mixin If not null, limit to results that have this mixin.
     */
    public Collection<FedoraResource> getChildren(final String mixin) throws FedoraException {
        Node mixinLiteral = null;
        if ( mixin != null ) {
            mixinLiteral = NodeFactory.createLiteral(mixin);
        }
        final ExtendedIterator<Triple> it = graph.find(Node.ANY, CONTAINS.asNode(), Node.ANY);
        final Set<FedoraResource> set = new HashSet<>();
        while (it.hasNext()) {
            final Node child = it.next().getObject();
            if ( mixin == null || graph.contains(child, HAS_MIXIN_TYPE.asNode(), mixinLiteral) ) {
                final String path = child.getURI().toString()
                        .replaceAll(repository.getRepositoryUrl(),"");
                if ( graph.contains(child, HAS_MIXIN_TYPE.asNode(), binaryType) ) {
                    set.add( repository.getDatastream(path) );
                } else {
                    set.add( repository.getObject(path) );
                }
            }
        }
        return set;
    }

    @Override
    public FedoraObject createObject() throws FedoraException {
        final HttpPost post = httpHelper.createPostMethod(getPath(), null);
        try {
            final HttpResponse response = httpHelper.execute(post);
            final String uri = post.getURI().toString();
            final StatusLine status = response.getStatusLine();
            final int statusCode = status.getStatusCode();

            if (statusCode == SC_CREATED) {
                return repository.getObject(response.getFirstHeader("Location").getValue().substring(
                        repository.getRepositoryUrl().length()));
            } else if (statusCode == SC_FORBIDDEN) {
                LOGGER.error("request to create resource {} is not authorized.", uri);
                throw new ForbiddenException("request to create resource " + uri + " is not authorized.");
            } else {
                LOGGER.error("error creating resource {}: {} {}", uri, statusCode, status.getReasonPhrase());
                throw new FedoraException("error retrieving resource " + uri + ": " + statusCode + " " +
                        status.getReasonPhrase());
            }
        } catch (final Exception e) {
            LOGGER.error("could not encode URI parameter", e);
            throw new FedoraException(e);
        } finally {
            post.releaseConnection();
        }
    }
}
