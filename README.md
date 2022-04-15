Kafka Connect SMT to add domain metadata extracted from a provided URL field 

This SMT supports inserting Domain metadata into the record Key or Value
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`uuid.field.name`| Field name for UUID | String | `uuid` | High |

Example on how to add to your connector:
```
transforms=domainmeta
transforms.domainmeta.type=com.github.lowess.kafka.connect.smt.InsertDomainMetadata$Value
transforms.domainmeta.url.field.name="page_url"
transforms.domainmeta.domain.field.name="domain"
transforms.domainmeta.top_domain.field.name="top_domain"
```

TODO

* ~~add support for records without schemas~~

Lots borrowed from the Apache Kafka® `InsertField` SMT
