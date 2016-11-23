import os
import pickle
from datetime import timedelta
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *


os.environ["SPARK_HOME"] = "/Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/site-packages/spark-2.0.1-bin-hadoop2.7"
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

conf = SparkConf().setMaster("local").setAppName("hint evaluate")
sc = SparkContext(conf = conf)

data_pkl = pickle.load(open('data.pkl', 'rb'))

# initial_datetime = datetime.datetime(2016, 11, 1, 0, 0, 0)
# for problem in data_pkl:
#     for student in data_pkl[problem]:
#         first_timestamp = 0
#         index = 0
#         for one_timestamp in data_pkl[problem][student]:
#             time = datetime.datetime.strptime(one_timestamp[0], '%Y-%m-%d %H:%M:%S')
#             if first_timestamp == 0:
#                 first_timestamp = time
#             new_timestamp = time - first_timestamp
#             data_pkl[problem][student][index][0] = str(initial_datetime + new_timestamp)
#             index += 1


# print(data_pkl['5', '1', '1'])

sqlContext = SQLContext(sc) # or HiveContext

student_df_list = []

print('grab data from data set...')
for week in range(5, 6):
    for problem in range(1, 11):
        for part in range(1, 11):
            if (str(week), str(problem), str(part)) not in data_pkl.keys():
                continue
            for student, record in data_pkl[str(week), str(problem), str(part)].items():
                student_record_df = sc.parallelize([
                    student + tuple(r)
                    for r in record
                ])
                student_df_list.append(student_record_df)
                # if student in student_df_list.keys():
                #     student_df_list[student] = student_df_list[student].union(student_record_df)
                # else:
                #     student_df_list[student] = student_record_df
                # student_df_list.append(student_record_df)

# print('construct data frame...')
# for student, student_record_df in student_df_list.items():
#     hasattr(student_record_df, "toDF")
#     student_record_df = student_record_df.toDF(['id', 'username', 'timestamp', 'attempt', 'score', 'answer'])
#     student_record_df.createOrReplaceTempView("student")
#     student_df_list[student] = sqlContext.sql("SELECT * FROM student order by timestamp")

# for student, record in data_pkl['6', '6', '2'].items():
#     student_record_df = sc.parallelize([
#         student + tuple(r)
#         for r in record
#     ])
#     student_df_list.append(student_record_df)


hint_dataset = ()
without_hint_dataset = ()
counter = 0
student_with_hint = 0
student_with_hint_attempt = 0
student_without_hint = 0
student_without_hint_attempt = 0
for student_record_df in student_df_list:
    # counter += 1
    # if counter == 10:
    #     break

    hasattr(student_record_df, "toDF")
    student_record_df = student_record_df.toDF(['id', 'username', 'timestamp', 'attempt', 'score', 'answer'])
    # student_record_df.show()
    student_record_df.createOrReplaceTempView("student")
    sqlDF = sqlContext.sql("SELECT * FROM student where answer = 'Hint' limit 1")
    if not sqlDF.rdd.isEmpty():
        print('----------------------------------------------')
        print('This student has a hint!')
        # hint_timestamp = datetime.datetime.strptime(sqlDF.rdd.map(lambda row: row.timestamp).first(), '%Y-%m-%d %H:%M:%S')
        hint_message = sqlDF.rdd.map(lambda row: (row.id, row.username, row.timestamp, row.score, row.attempt)).first()
        hint_timestamp = hint_message[2]
        print('hint_timestamp :' + str(hint_timestamp))
        # sqlDF.show()

        sqlDF = sqlContext.sql("SELECT * FROM student where score != 0 limit 1")
        if not sqlDF.rdd.isEmpty():

            # hint_first_correct_timestamp = datetime.datetime.strptime(sqlDF.rdd.map(lambda row: row.timestamp).first(), '%Y-%m-%d %H:%M:%S')
            correct_message = sqlDF.rdd.map(lambda row: (row.answer, row.timestamp)).first()
            hint_first_correct_timestamp = correct_message[1]
            print('hint_first_correct_timestamp : ' + hint_first_correct_timestamp)
            # sqlDF.filter("timestamp >= '" + str(hint_timestamp) + "'").filter("timestamp <= '" + str(hint_first_correct_timestamp) + "'").count().show()
            student_record_df.show()
            sqlDF = sqlContext.sql("select * from student where answer != 'Hint' and timestamp between '" + str(hint_timestamp) + "' and '" + str(hint_first_correct_timestamp) + "'")
            if sqlDF.count() > 0:
                student_with_hint += 1
                student_with_hint_attempt += sqlDF.count()
            print(sqlDF.count())
            # sqlDF.show()
            # time_length = datetime.datetime.strptime(hint_first_correct_timestamp, '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(hint_timestamp, '%Y-%m-%d %H:%M:%S')
            # hint_message += correct_message + (str(time_length),)
            # hint_dataset += (hint_message,)
            # print(hint_message)
    else:

        sqlDF = sqlContext.sql("SELECT * FROM student where score = 0 limit 1")
        if sqlDF.rdd.isEmpty():
            continue

        without_hint_message = sqlDF.rdd.map(lambda row: (row.id, row.username, row.timestamp, row.attempt, row.answer)).first()
        first_timestamp = datetime.datetime.strptime(without_hint_message[2],
                                                                 '%Y-%m-%d %H:%M:%S')
        print('----------------------------------------------')
        print('This student does NOT have a hint!')
        print('first_timestamp : ' + str(first_timestamp))
        # sqlDF.show()

        sqlDF = sqlContext.sql("SELECT * FROM student where score != 0 limit 1")
        if sqlDF.rdd.isEmpty():
            continue
        without_hint_correct_message = (sqlDF.rdd.map(lambda row: row.timestamp).first(),)
        first_correct_timestamp = datetime.datetime.strptime(without_hint_correct_message[0],
                                                     '%Y-%m-%d %H:%M:%S')
        print('first_correct_timestamp : ' + str(first_correct_timestamp))
        # sqlDF.filter("timestamp >= '" + str(first_timestamp) + "'").filter(
        #     "timestamp <= '" + str(first_correct_timestamp) + "'").count().show()

        student_record_df.show()
        sqlDF = sqlContext.sql("select * from student where timestamp between '" + str(first_timestamp) + "' and '" + str(first_correct_timestamp) + "'")
        if sqlDF.count() > 0:
            student_without_hint += 1
            student_without_hint_attempt += sqlDF.count()

        print(sqlDF.count())


print('number of problem without hint is ' + str(student_without_hint))
print('total attempt is ' + str(student_without_hint_attempt))
print('average is ' + str(student_without_hint_attempt / student_without_hint))


print('number of problem with hint is ' + str(student_with_hint))
print('total attempt is ' + str(student_with_hint_attempt))
print('average is ' + str(student_with_hint_attempt / student_with_hint))