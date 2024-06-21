CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
                        id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                        object_id varchar NOT NULL UNIQUE,
                        object_value text NOT NULL,
                        update_ts timestamp NOT NULL
                    );

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
                        id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                        object_id varchar NOT NULL UNIQUE,
                        object_value text NOT NULL,
                        update_ts timestamp NOT NULL
                    );