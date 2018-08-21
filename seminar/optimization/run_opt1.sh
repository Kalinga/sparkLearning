echo "Query 1"
echo "Cold:"
sql database_kara9147_opt1 < clearBuffer.sql>/dev/null
time sql database_kara9147_opt1 <q1.sql >/dev/null
echo "Hot:"
time sql database_kara9147_opt1 <q1.sql >/dev/null

echo "External Table"
echo "Cold:"
sql database_kara9147_opt1 < clearBuffer.sql>/dev/null
time sql database_kara9147_opt1 <q1Ext.sql >/dev/null
echo "Hot:"
time sql database_kara9147_opt1 <q1Ext.sql >/dev/null



echo "Query 2"
echo "Cold:"
sql database_kara9147_opt1 < clearBuffer.sql>/dev/null
time sql database_kara9147_opt1 <q2.sql >/dev/null
echo "Hot:"
time sql database_kara9147_opt1 <q2.sql >/dev/null

echo "External Table"
echo "Cold:"
sql database_kara9147_opt1 < clearBuffer.sql>/dev/null
time sql database_kara9147_opt1 <q2Ext.sql >/dev/null
echo "Hot:"
time sql database_kara9147_opt1 <q2Ext.sql >/dev/null



echo "Query 3"
echo "Cold:"
sql database_kara9147_opt1 < clearBuffer.sql>/dev/null
time sql database_kara9147_opt1 <q3.sql >/dev/null
echo "Hot:"
time sql database_kara9147_opt1 <q3.sql >/dev/null
echo "External Table"
echo "Cold:"
sql database_kara9147_opt1 < clearBuffer.sql>/dev/null
time sql database_kara9147_opt1 <q3Ext.sql >/dev/null
echo "Hot:"
time sql database_kara9147_opt1 <q3Ext.sql >/dev/null
