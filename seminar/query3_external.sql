WITH output AS  (SELECT UV._c1 as sourceIP,
          AVG(R._c2) as avgPageRank,
          SUM(UV._c4) as totalRevenue
    FROM Rankings_ext AS R, UserVisits_ext AS UV
    WHERE R._c1 = UV._c2
       AND UV._c3 BETWEEN Date('1980-01-01') AND Date('1980-04-01')
    GROUP BY UV._c1)

SELECT sourceIP, totalRevenue, avgPageRank
FROM output
  ORDER BY totalRevenue  
\g
