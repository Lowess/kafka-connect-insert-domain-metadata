Kafka Connect SMT to add domain metadata extracted from a provided URL field 

This SMT supports inserting Domain metadata into the record Key or Value
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`url.field.name`| Field name for source URL | String | `url` | High |
|`domain.field.name`| Field name for domain generation | String | `domain` | Low |
|`top_domain.field.name`| Field name for top domain generation | String | `top_domain` | Low |

Example on how to add to your connector:
```
transforms=domainmeta
transforms.domainmeta.type=com.github.lowess.kafka.connect.smt.InsertDomainMetadata$Value
transforms.domainmeta.url.field.name="page_url"
transforms.domainmeta.domain.field.name="domain"
transforms.domainmeta.top_domain.field.name="top_domain"
```

Lots borrowed from the Apache Kafka® `InsertField` SMT
