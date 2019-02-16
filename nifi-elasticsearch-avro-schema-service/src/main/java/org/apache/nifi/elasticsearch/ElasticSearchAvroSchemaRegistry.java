package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ElasticSearchAvroSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);

    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("es-client-service")
        .displayName("Client Service")
        .description("The ElasticSearch client service to use for retrieving data.")
        .identifiesControllerService(ElasticSearchClientService.class)
        .addValidator(Validator.VALID)
        .required(true)
        .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
        .name("es-index")
        .displayName("Index")
        .description("The ElasticSearch index to target.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("avro_schemas")
        .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
        .name("es-type")
        .displayName("Type")
        .description("The ElasticSearch type to target.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("schema")
        .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CLIENT_SERVICE, INDEX, TYPE
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile ElasticSearchClientService clientService;
    private volatile String index;
    private volatile String type;
    private static ObjectMapper MAPPER = new ObjectMapper();

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
        index = context.getProperty(INDEX).evaluateAttributeExpressions().getValue();
        type  = context.getProperty(TYPE).evaluateAttributeExpressions().getValue();
    }

    private String serializeQuery(String name, Integer version) throws IOException {
        Map<String, Object> query = new HashMap<String, Object>(){{
            put("size", 1);
            put("sort", new ArrayList(){{
                add(new HashMap<String, Object>(){{
                    put("version", new HashMap<String, Object>(){{
                        put("order", "desc");
                    }});
                }});
            }});
            put("query", new HashMap<String, Object>(){{
                put("bool", new HashMap<String, Object>(){{
                    put("must", new ArrayList<Map<String, Object>>(){{
                        add(new HashMap<String, Object>(){{
                            put("match", new HashMap<String, Object>(){{
                                put("name", name);
                            }});
                        }});
                        if (version != null) {
                            add(new HashMap<String, Object>() {{
                                put("match", new HashMap<String, Object>() {{
                                    put("version", version);
                                }});
                            }});
                        }
                    }});
                }});
            }});
        }};

        String serialized = MAPPER.writeValueAsString(query);
        if (getLogger().isDebugEnabled()) {
            getLogger().debug(String.format("Built this query:\n%s", serialized));
        }
        return serialized;
    }

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        Integer version = schemaIdentifier.getVersion().isPresent() ? schemaIdentifier.getVersion().getAsInt() : null;
        String query = serializeQuery(schemaIdentifier.getName().get(), version);
        SearchResponse response = clientService.search(query, index, type);

        List<Map<String, Object>> hits = response.getHits();
        if (hits == null || hits.size() != 1) {
            throw new ProcessException(String.format("Schema not found; hit count was %d", hits != null ? hits.size() : 0));
        }

        String text = (String)((Map<String, Object>)hits.get(0).get("_source")).get("text");

        Schema schema = new Schema.Parser().parse(text);

        return AvroTypeUtil.createSchema(schema);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
