spark-submit --master yarn --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/home/actian/spark_vector_loader-assembly-2.0-SNAPSHOT.jar load csv -sf /user/actian/amplab_data/uservisits -vh 172.21.249.73 -vi VH -vd database_kara9147_opt1 -tt uservisits -sc ","
sql database_kara9147_opt1 < userVistCount.sql 

