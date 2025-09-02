INSERT INTO `Ludovic`.`replication_test_1`.`my-source-topic` 
SELECT key, val 
FROM `Ludovic`.`replication_test_2`.`my-target-topic`
/*+ OPTIONS(
  'scan.startup.mode'='specific-offsets',
  'scan.startup.specific-offsets'='partition:2,offset:11371893;partition:3,offset:10619260;partition:4,offset:10969589;partition:0,offset:11237882;partition:1,offset:11038048;partition:5,offset:11414363'
) */;