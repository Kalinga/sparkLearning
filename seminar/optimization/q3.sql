call vectorwise(clear_bm) \g 
WITH output AS  (SELECT sourceIP,
          AVG(pageRank) as avgPageRank,
          SUM(adRevenue) as totalRevenue
    FROM Rankings AS R, UserVisits AS UV
    WHERE R.pageURL = UV.destURL
       AND UV.visitDate BETWEEN Date('1970-01-01') AND Date('1970-01-04')
    GROUP BY UV.sourceIP)

SELECT TOP 1 sourceIP, totalRevenue, avgPageRank
FROM output
  ORDER BY totalRevenue  
\g
