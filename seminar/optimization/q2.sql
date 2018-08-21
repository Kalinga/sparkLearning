call vectorwise(clear_bm) \g 
SELECT SUBSTR(sourceIP, 1, 10), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 10) \g
