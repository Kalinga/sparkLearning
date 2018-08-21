CREATE EXTERNAL TABLE Rankings_ext (_c0 VARCHAR(300) NOT NULL, _c1 INT, _c2 INT) USING SPARK WITH REFERENCE='hdfs://dbblade13.prakinf.tu-ilmenau.de:8020/user/actian/amplab_data/rankings' , format = 'csv', OPTIONS=('delimiter' = ',' ) \g

CREATE EXTERNAL TABLE UserVisits_ext (_c0 VARCHAR(116) NOT NULL, _c1 VARCHAR(100), _c2 DATE, _c3 FLOAT, _c4 VARCHAR(256), _c5 CHAR(3), _c6 CHAR(6) , _c7 VARCHAR(32), _c8 INT) USING SPARK WITH REFERENCE='hdfs://dbblade13.prakinf.tu-ilmenau.de:8020/user/actian/amplab_data/uservisits' , format = 'csv', OPTIONS=('delimiter' = ',' ) \g

