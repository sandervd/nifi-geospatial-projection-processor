/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package be.vlaanderen.informatievlaanderen.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Tags({"ldes, vsds, geospatial, rdf"})
@CapabilityDescription("Performs a geospatial re-projection on RDF FlowFile")
public class GeospatialProjectionProcessor extends AbstractProcessor {
    public static final ValueFactory vf = SimpleValueFactory.getInstance();
    public static IRI wktLiteral = vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral");

    public static final PropertyDescriptor TARGET_CRS = new PropertyDescriptor
            .Builder().name("TARGET_CRS")
            .displayName("Target CRS")
            .required(true)
            .defaultValue("WGS84")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(TARGET_CRS);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        String CRS_To = context.getProperty(TARGET_CRS).getValue();

        if ( flowFile == null ) {
            return;
        }
        StringWriter outputStream = new StringWriter();
        AtomicBoolean executedSuccessfully = new AtomicBoolean(false);
        AtomicBoolean hasChange = new AtomicBoolean(false);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream RDFStream) throws IOException {
                try{
                    Model inputModel = Rio.parse(RDFStream, "", RDFFormat.NQUADS);
                    Model transformedModel = geoTransform(inputModel, CRS_To);
                    if (!Models.isomorphic(inputModel, transformedModel)) {
                        hasChange.set(true);
                        Rio.write(inputModel, outputStream, RDFFormat.NQUADS);
                    }
                    executedSuccessfully.set(true);
                }
                catch (Exception e) {
                    getLogger().error("Error executing geo projection.");
                }
            }
        });
        if (executedSuccessfully.get()) {
            if (hasChange.get())
                flowFile = session.write(flowFile, out -> out.write(outputStream.toString().getBytes()));
            session.transfer(flowFile, REL_SUCCESS);
        }
        else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    public static Model geoTransform(Model inputModel, String CRS_To) throws MalformedURLException {
        final URL host = new URL("http://localhost:8090/");

        inputModel.getStatements(null, null,null).forEach( statement -> {
            if (!statement.getObject().isLiteral())
                return;
            Literal object = (Literal) statement.getObject();

            if (!object.getDatatype().equals(wktLiteral))
                return;
            String obj = object.stringValue();
            String pattern = "^<(.*)> (.*)$";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(obj);
            if (!m.find())
                return;

            String CRS_From = m.group(1);
            String Geometry_from = m.group(2);

            try {
                String Geometry_to = doRequest(host, CRS_From, CRS_To, Geometry_from);
                inputModel.remove(statement.getSubject(), statement.getPredicate(), object);
                Literal new_object = vf.createLiteral("<" + CRS_To + "> " + Geometry_to, wktLiteral);
                inputModel.add(vf.createStatement(statement.getSubject(), statement.getPredicate(), new_object));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return inputModel;
    }

    public static String doRequest(URL EndpointURL, String CRS_From, String CRS_To, String WKT) throws IOException {
        HttpURLConnection HTTPConnection = (HttpURLConnection) EndpointURL.openConnection();
        HTTPConnection.setRequestMethod("POST");
        HTTPConnection.setRequestProperty("Content-Type", "application/json");
        HTTPConnection.setDoOutput(true);

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("crs_from", CRS_From);
        jsonObj.put("crs_to", CRS_To);
        jsonObj.put("wkt", WKT);

        try(OutputStream os = HTTPConnection.getOutputStream()) {
            byte[] input = jsonObj.toString().getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(HTTPConnection.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
        }
    }
}
