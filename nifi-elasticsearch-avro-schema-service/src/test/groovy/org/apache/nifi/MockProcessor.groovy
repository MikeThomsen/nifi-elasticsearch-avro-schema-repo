package org.apache.nifi

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.Validator
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.schemaregistry.services.SchemaRegistry

class MockProcessor extends AbstractProcessor {
    static final PropertyDescriptor REGISTRY = new PropertyDescriptor.Builder()
        .name("registry")
        .identifiesControllerService(SchemaRegistry.class)
        .addValidator(Validator.VALID)
        .required(true)
        .build()

    static final List<PropertyDescriptor> PROPERTIES = [ REGISTRY ]

    @Override
    List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        PROPERTIES
    }

    @Override
    void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

    }
}
