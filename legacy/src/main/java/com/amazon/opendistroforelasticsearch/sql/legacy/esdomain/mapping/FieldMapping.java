/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.legacy.esdomain.mapping;

import com.amazon.opendistroforelasticsearch.sql.legacy.domain.Field;
import com.amazon.opendistroforelasticsearch.sql.legacy.utils.StringUtils;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;

/**
 * Field mapping that parses native ES mapping.
 * <p>
 * NOTE that approaches in this class are NOT reliable because of the ES mapping query API used.
 * We should deprecate this in future and parse field mapping in more solid way.
 */
public class FieldMapping {

    /**
     * Name of the Field to be parsed
     */
    private final String fieldName;

    /**
     * Native mapping information returned from ES
     */
    private final Map<String, FieldMappingMetadata> typeMappings;

    /**
     * Maps a field name to Field object that specified in query explicitly
     */
    private final Map<String, Field> specifiedFieldsByName;

    public FieldMapping(String fieldName) {
        this(fieldName, emptyMap(), emptyMap());
    }

    public FieldMapping(String fieldName,
                        Map<String, FieldMappingMetadata> typeMappings,
                        Map<String, Field> specifiedFieldByNames) {

        this.fieldName = fieldName;
        this.typeMappings = typeMappings;
        this.specifiedFieldsByName = specifiedFieldByNames;
    }

    /**
     * Is field specified explicitly in query
     *
     * @return true if specified
     */
    public boolean isSpecified() {
        return specifiedFieldsByName.containsKey(fieldName);
    }

    /**
     * Verify if property field matches wildcard pattern specified in query
     *
     * @return true if matched
     */
    public boolean isWildcardSpecified() {
        return specifiedFieldsByName.containsKey(path() + ".*");
    }

    /**
     * Is field a property field, which means either object field or nested field.
     *
     * @return true for property field
     */
    public boolean isPropertyField() {
        int numOfDots = StringUtils.countMatches(fieldName, '.');
        return numOfDots > 1 || (numOfDots == 1 && !isMultiField());
    }

    /**
     * Is field a/in multi-field, for example, field "a.keyword" in field "a"
     *
     * @return true for multi field
     */
    public boolean isMultiField() {
        return fieldName.endsWith(".keyword");
    }

    /**
     * Is field meta field, such as _id, _index, _source etc.
     *
     * @return true for meta field
     */
    public boolean isMetaField() {
        return fieldName.startsWith("_");
    }

    /**
     * Path of property field, for example "employee" in "employee.manager"
     *
     * @return path of property field
     */
    public String path() {
        int lastDot = fieldName.lastIndexOf(".");
        if (lastDot == -1) {
            throw new IllegalStateException("path() is being invoked on the wrong field [" + fieldName + "]");
        }
        return fieldName.substring(0, lastDot);
    }

    /**
     * Used to retrieve the type of fields from metaData map structures for both regular and nested fields
     */
    @SuppressWarnings("unchecked")
    public String type() {
        FieldMappingMetadata metaData = typeMappings.get(fieldName);
        Map<String, Object> source = metaData.sourceAsMap();
        String[] fieldPath = fieldName.split("\\.");

        /*
         * When field is not nested the metaData source is fieldName -> type
         * When it is nested or contains "." in general (ex. fieldName.nestedName) the source is nestedName -> type
         */
        String root = (fieldPath.length == 1) ? fieldName : fieldPath[1];
        Map<String, Object> fieldMapping = (Map<String, Object>) source.get(root);
        for (int i = 2; i < fieldPath.length; i++) {
            fieldMapping = (Map<String, Object>) fieldMapping.get(fieldPath[i]);
        }

        return (String) fieldMapping.get("type");
    }

}
