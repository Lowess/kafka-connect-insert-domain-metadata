/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.lowess.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Collections;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertDomainMetadata<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC = "Insert a TLD domain into a connect record";

  private interface ConfigName {
    String URL_FIELD_NAME = "url.field.name";
    String DOMAIN_FIELD_NAME = "domain.field.name";
    String TOP_DOMAIN_FIELD_NAME = "top_domain.field.name";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ConfigName.URL_FIELD_NAME, ConfigDef.Type.STRING, "url",
          ConfigDef.Importance.HIGH,
          "Field name where a URL can be found")
      .define(ConfigName.DOMAIN_FIELD_NAME, ConfigDef.Type.STRING, "domain", ConfigDef.Importance.HIGH,
          "Field name for the domain")
      .define(ConfigName.TOP_DOMAIN_FIELD_NAME, ConfigDef.Type.STRING, "domain", ConfigDef.Importance.HIGH,
          "Field name for the top domain");

  private static final String PURPOSE = "Adding domain metadata to record";

  private String urlFieldName;
  private String domainFieldName;
  private String topDomainFieldName;

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    domainFieldName = config.getString(ConfigName.DOMAIN_FIELD_NAME);
    topDomainFieldName = config.getString(ConfigName.TOP_DOMAIN_FIELD_NAME);
    urlFieldName = config.getString(ConfigName.URL_FIELD_NAME);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

    final Map<String, Object> updatedValue = new HashMap<>(value);

    final String domain = getDomain(value.get(urlFieldName).toString());
    updatedValue.put(domainFieldName, domain);
    updatedValue.put(topDomainFieldName, getTopDomain(domain));

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema());
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      updatedValue.put(field.name(), value.get(field));
    }

    final String domain = getDomain(value.get(urlFieldName).toString());
    updatedValue.put(domainFieldName, domain);
    updatedValue.put(topDomainFieldName, getTopDomain(domain));

    return newRecord(record, updatedSchema, updatedValue);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private String getDomain(String url) {
    try {
      return new URI(url).getHost();
    } catch (URISyntaxException e) {
      return "unknown";
    }
  }

  private String getTopDomain(String domain) {

    ArrayList<String> domainSplit = new ArrayList<String>(Arrays.asList(domain.split("\\.")));

    if (domainSplit.size() > 2) {
      Collections.reverse(domainSplit);
      return getTopDomain(domainSplit.get(1) + "." + domainSplit.get(0));
    }

    return domain;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(domainFieldName, Schema.STRING_SCHEMA);
    builder.field(topDomainFieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends InsertDomainMetadata<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
          record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends InsertDomainMetadata<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema,
          updatedValue, record.timestamp());
    }
  }
}
