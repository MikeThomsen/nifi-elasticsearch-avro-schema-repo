package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ElasticSearchAvroSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
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
        .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
        .name("es-type")
        .displayName("Type")
        .description("The ElasticSearch type to target.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        return null;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return null;
    }
}
