SELECT SUBSTR(sourceIP, 1, 12), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 12) \g

