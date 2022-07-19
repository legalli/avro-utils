package com.github.legalli.avroUtils;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class JsonToAvroSizeComparator {

    public static void main(String... varargs) {

        // Place your Avro schema here
        String schema = """
                {
                    "type": "record",
                    "name": "domain",
                    "namespace": "type",
                    "fields": [
                        {
                            "name": "data",
                            "type": {
                                "name": "data_record",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "attributes",
                                        "type": {
                                            "name": "attributes_record",
                                            "type": "record",
                                            "fields": [
                                                {
                                                    "type": "string",
                                                    "name": "user_id"
                                                },
                                                {
                                                    "type": "string",
                                                    "name": "type"
                                                },
                                                {
                                                    "type": "string",
                                                    "name": "category"
                                                },
                                                {
                                                    "type": "long",
                                                    "name": "scheduled_for"
                                                },
                                                {
                                                    "type":  "string",
                                                    "name": "payload"
                                                },
                                                {
                                                    "type": "string",
                                                    "name": "localized_string_key"
                                                },
                                                {
                                                    "type": "long",
                                                    "name": "created_at"
                                                },
                                                {
                                                    "type": "long",
                                                    "name": "scheduled_at"
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "type": "string",
                                        "name": "id"
                                    },
                                    {
                                        "type": "string",
                                        "name": "type"
                                    }
                                ]
                            }
                        },
                        {
                            "name": "meta",
                            "type": {
                                "name": "meta_record",
                                "type": "record",
                                "fields": [
                                    {
                                        "type": "string",
                                        "name": "correlation_id"
                                    },
                                    {
                                        "type": "long",
                                        "name": "created_at"
                                    },
                                    {
                                        "type": "string",
                                        "name": "service"
                                    }
                                ]
                            }
                        }
                    ]
                }
                """;

        // Place your JSON here
        String json = """
                {
                    "data": {
                        "attributes": {
                            "user_id": "a9867762-4d7e-4f63-94af-0a19dea64eba",
                            "type": "push-type",
                            "category": "category-id",
                            "scheduled_for": 1655374567944,
                            "payload": "Your charger is ready!",
                            "localized_string_key": "key",
                            "created_at": 1655374567944,
                            "scheduled_at": 1655374567944
                        },
                        "id": "a9867762-4d7e-4f63-94af-0a19dea64eba",
                        "type": "wallbox.notification.push_notification_scheduled"
                    },
                    "meta": {
                        "correlation_id": "correlation-id",
                        "created_at": 1655374567944,
                        "service": "wallbox-api"
                    }
                }
                """;

        try {
            // conversion to binary Avro
            final var converter = new JsonAvroConverter();
            byte[] avro = converter.convertToAvro(json.getBytes(), schema);

            // print output
            System.out.println("JSON Message ==========================================");
            System.out.println(json);
            System.out.println("AVRO Message ==========================================");
            System.out.println(avro.toString());
            System.out.println("Comparison ============================================");
            System.out.println("Avro size:" + avro.length);
            System.out.println("JSON size:" + json.length());
            System.out.println("Avro to JSON size ratio: " + String.format("%.2f", (double)avro.length / (double)json.length()));
            System.out.println("End ===================================================");
        } catch (Exception e) {
            System.out.println("Error" + e.toString());
        }


    }

}
