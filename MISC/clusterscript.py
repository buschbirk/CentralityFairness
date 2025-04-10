

import os
os.environ['SPARK_HOME']='/home/laal/spark-3.0.2-bin-hadoop2.7'
os.environ['SPARK_LOCAL_DIRS']='/home/laal/TMP'
os.environ['LOCAL_DIRS']=os.environ['SPARK_LOCAL_DIRS']
os.environ['SPARK_WORKER_DIR']=os.path.join(os.environ['SPARK_LOCAL_DIRS'], 'work')
os.environ["JAVA_HOME"] = "/home/laal/jdk-9.0.4"

from sparkhpc import sparkjob

sparkjob.start_cluster('20000M', 
                       cores_per_executor=4, 
                       spark_home=os.environ['SPARK_HOME'])






