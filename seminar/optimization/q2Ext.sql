call vectorwise(clear_bm) \g 

SELECT SUBSTR(_c0, 1, 10), SUM(_c3) FROM uservisits_ext GROUP BY SUBSTR(_c0, 1, 10) \g
