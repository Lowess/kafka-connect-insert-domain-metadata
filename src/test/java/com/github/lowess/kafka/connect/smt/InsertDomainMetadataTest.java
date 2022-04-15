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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.github.lowess.kafka.connect.smt.InsertDomainMetadata;

import static org.junit.Assert.*;

public class InsertDomainMetadataTest {

  private InsertDomainMetadata<SourceRecord> xform = new InsertDomainMetadata.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test(expected = DataException.class)
  public void topLevelStructRequired() {
    xform.configure(Collections.singletonMap("domain.field.name", "domain"));
    xform.configure(Collections.singletonMap("top_domain.field.name", "domain"));
    xform.configure(Collections.singletonMap("url.field.name", "url"));
    xform.apply(new SourceRecord(null, null, "", 0, Schema.STRING_SCHEMA, "http://example.com"));
  }

  @Test
  public void copySchemaAndInsertTopLevelDomainField() {
    final Map<String, Object> props = new HashMap<>();

    props.put("url.field.name", "url");
    props.put("domain.field.name", "domain");
    props.put("top_domain.field.name", "top_domain");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
        .field("url", Schema.STRING_SCHEMA);

    final Struct simpleStruct = new Struct(simpleStructSchema).put("url",
        "https://subdomain.example.com/some/path?q=with_qparams");

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
        simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(),
        transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(),
        transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(),
        transformedRecord.valueSchema().doc());

    assertEquals(Schema.STRING_SCHEMA,
        transformedRecord.valueSchema().field("url").schema());

    assertEquals("subdomain.example.com",
        ((Struct) transformedRecord.value()).getString("domain"));
    assertEquals(Schema.STRING_SCHEMA,
        transformedRecord.valueSchema().field("domain").schema());
    assertNotNull(((Struct) transformedRecord.value()).getString("domain"));

    assertEquals("example.com",
        ((Struct) transformedRecord.value()).getString("top_domain"));
    assertEquals(Schema.STRING_SCHEMA,
        transformedRecord.valueSchema().field("top_domain").schema());
    assertNotNull(((Struct) transformedRecord.value()).getString("top_domain"));
  }

  @Test
  public void schemalessInsertTopLevelDomainField() {
    final Map<String, Object> props = new HashMap<>();

    props.put("url.field.name", "url");
    props.put("domain.field.name", "domain");
    props.put("top_domain.field.name", "top_domain");

    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
        null, Collections.singletonMap("url", "http://example.com"));

    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals("example.com", ((Map) transformedRecord.value()).get("domain"));
    assertNotNull(((Map) transformedRecord.value()).get("domain"));

    assertEquals("example.com", ((Map) transformedRecord.value()).get("top_domain"));
    assertNotNull(((Map) transformedRecord.value()).get("top_domain"));
  }
}