SELECT SUBSTR(_c1, 1, 10), SUM(_c4) FROM uservisits_ext GROUP BY SUBSTR(_c1, 1, 10) \g

