export SPARK_VERSION=3.4
spark-sql --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.1 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'



CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
PARTITIONED BY (city);


INSERT INTO hudi_table
VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'    ),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'    ),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'      ),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');

-- bulk_insert using INSERT_INTO 
SET hoodie.spark.sql.insert.into.operation = 'bulk_insert' 
-- This throws an error during next insert


-- Update data
UPDATE hudi_table SET fare = 25 WHERE fare = 27.70;

-- source table using Hudi for testing merging into target Hudi table
CREATE TABLE fare_adjustment (ts BIGINT, uuid STRING, rider STRING, driver STRING, fare DOUBLE, city STRING) 
USING HUDI;

INSERT INTO fare_adjustment VALUES 
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',-2.70 ,'san_francisco'),
(1695530237068,'3f3d9565-7261-40e6-9b39-b8aa784f95e2','rider-K','driver-U',64.20 ,'san_francisco'),
(1695241330902,'ea4c36ff-2069-4148-9927-ef8c1a5abd24','rider-H','driver-R',66.60 ,'sao_paulo'),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',1.85,'chennai');


MERGE INTO hudi_table AS target
USING fare_adjustment AS source
ON target.uuid = source.uuid
WHEN MATCHED THEN UPDATE SET target.fare = target.fare + source.fare
WHEN NOT MATCHED THEN INSERT *
;

--DELETE data
DELETE FROM hudi_table WHERE uuid = '3f3d9565-7261-40e6-9b39-b8aa784f95e2';

--Time Travel

-- time travel based on commit time, for eg: `20220307091628793`

SELECT * FROM hudi_table TIMESTAMP AS OF '2024-02-05 13:30:00.100' uuid = '3f3d9565-7261-40e6-9b39-b8aa784f95e2';

