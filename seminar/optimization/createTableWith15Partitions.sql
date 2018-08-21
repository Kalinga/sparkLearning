CREATE TABLE Rankings ( 
pageURL VARCHAR(300),
pageRank INT,
avgDuration INT) WITH PARTITION = (HASH ON pageURL 15 PARTITIONS);
\g

CREATE TABLE Uservisits ( 
sourceIP VARCHAR(116),
destURL VARCHAR(100),
visitDate DATE,
adRevenue FLOAT,
userAgent VARCHAR(256),
countryCode CHAR(3),
languageCode CHAR(6),
searchWord VARCHAR(32),
duration INT) WITH PARTITION = (HASH ON destURL 15 PARTITIONS);
\g

