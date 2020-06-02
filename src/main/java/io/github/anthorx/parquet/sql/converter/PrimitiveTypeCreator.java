package io.github.anthorx.parquet.sql.converter;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class PrimitiveTypeCreator {
    public static PrimitiveType create(String name,
                                       PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                       LogicalTypeAnnotation logicalTypeAnnotation,
                                       Boolean nullable) {
        Types.PrimitiveBuilder<PrimitiveType> primitiveBuilder;
        if (nullable) {
            primitiveBuilder = org.apache.parquet.schema.Types.optional(primitiveTypeName);
        } else {
            primitiveBuilder = org.apache.parquet.schema.Types.required(primitiveTypeName);
        }

        if (logicalTypeAnnotation != null) {
            primitiveBuilder.as(logicalTypeAnnotation);
        }

        return primitiveBuilder.named(name);
    }
}
