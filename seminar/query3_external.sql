WITH output AS  (SELECT UV._c0 as sourceIP,
          AVG(R._c1) as avgPageRank,
          SUM(UV._c3) as totalRevenue
    FROM Rankings_ext AS R, UserVisits_ext AS UV
    WHERE R._c0 = UV._c1
       AND UV._c2 BETWEEN Date('1970-01-01') AND Date('1970-01-04')
    GROUP BY UV._c0)

SELECT TOP 1 sourceIP, totalRevenue, avgPageRank
FROM output
  ORDER BY totalRevenue DESC 
\g
