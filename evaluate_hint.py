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
initial_datetime = datetime.datetime(2016, 11, 1, 0, 0, 0)

for problem in data_pkl:
    for student in data_pkl[problem]:
        first_timestamp = 0
        index = 0
        for one_timestamp in data_pkl[problem][student]:
            time = datetime.datetime.strptime(one_timestamp[0], '%Y-%m-%d %H:%M:%S')
            if first_timestamp == 0:
                first_timestamp = time
            new_timestamp = time - first_timestamp
            data_pkl[problem][student][index][0] = str(initial_datetime + new_timestamp)
            index += 1


# print(data_pkl['5', '1', '1'])

sqlContext = SQLContext(sc) # or HiveContext

student_df_list = {}

for week in range(6, 7):
    for problem in range(1, 2):
        for part in range(1, 10):
            if (str(week), str(problem), str(part)) not in data_pkl.keys():
                continue
            for student, record in data_pkl[str(week), str(problem), str(part)].items():
                student_record_df = sc.parallelize([
                    student + tuple(r)
                    for r in record
                ])
                if student in student_df_list.keys():
                    student_df_list[student] = student_df_list[student].union(student_record_df)
                else:
                    student_df_list[student] = student_record_df
                # student_df_list.append(student_record_df)


for student, student_record_df in student_df_list.items():
    hasattr(student_record_df, "toDF")
    student_record_df = student_record_df.toDF(['id', 'username', 'timestamp', 'attempt', 'score', 'answer'])
    student_record_df.createOrReplaceTempView("student")
    student_df_list[student] = sqlContext.sql("SELECT * FROM student order by timestamp desc")

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
student_without_hint = 0
for student, student_record_df in student_df_list.items():
    # counter += 1
    # if counter == 10:
    #     break
    print('----------------------------------------------')
    # hasattr(student_record_df, "toDF")
    # student_record_df = student_record_df.toDF(['id', 'username', 'timestamp', 'attempt', 'score', 'answer'])
    student_record_df.show()
    student_record_df.createOrReplaceTempView("student")
    sqlDF = sqlContext.sql("SELECT * FROM student where answer = 'Hint' limit 1")
    if not sqlDF.rdd.isEmpty():

        print('This student has a hint!')
        # hint_timestamp = datetime.datetime.strptime(sqlDF.rdd.map(lambda row: row.timestamp).first(), '%Y-%m-%d %H:%M:%S')
        hint_message = sqlDF.rdd.map(lambda row: (row.id, row.username, row.timestamp, row.score, row.attempt)).first()
        hint_timestamp = hint_message[2]
        print('hint_timestamp :' + str(hint_timestamp))
        # sqlDF.show()

        sqlDF = sqlContext.sql("SELECT * FROM student where score != 0 limit 1")
        if not sqlDF.rdd.isEmpty():
            student_with_hint += 1
            # hint_first_correct_timestamp = datetime.datetime.strptime(sqlDF.rdd.map(lambda row: row.timestamp).first(), '%Y-%m-%d %H:%M:%S')
            correct_message = sqlDF.rdd.map(lambda row: (row.answer, row.timestamp)).first()
            hint_first_correct_timestamp = correct_message[1]
            print('hint_first_correct_timestamp : ' + hint_first_correct_timestamp)
            # sqlDF.show()
            time_length = datetime.datetime.strptime(hint_first_correct_timestamp, '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(hint_timestamp, '%Y-%m-%d %H:%M:%S')
            hint_message += correct_message + (str(time_length),)
            hint_dataset += (hint_message,)
            print(hint_message)
    else:
        print('This student does NOT have a hint!')
        sqlDF = sqlContext.sql("SELECT * FROM student where score = 0 limit 1")
        if sqlDF.rdd.isEmpty():
            continue

        without_hint_message = sqlDF.rdd.map(lambda row: (row.id, row.username, row.timestamp, row.attempt, row.answer)).first()
        first_timestamp = datetime.datetime.strptime(without_hint_message[2],
                                                                 '%Y-%m-%d %H:%M:%S')
        print('first_timestamp : ' + str(first_timestamp))
        # sqlDF.show()

        sqlDF = sqlContext.sql("SELECT * FROM student where score != 0 limit 1")
        if sqlDF.rdd.isEmpty():
            continue
        student_without_hint += 1
        without_hint_correct_message = (sqlDF.rdd.map(lambda row: row.timestamp).first(),)
        first_correct_timestamp = datetime.datetime.strptime(without_hint_correct_message[0],
                                                     '%Y-%m-%d %H:%M:%S')
        print('first_correct_timestamp : ' + str(first_correct_timestamp))

        time_length = first_correct_timestamp - first_timestamp
        without_hint_message += without_hint_correct_message + (str(time_length),)
        without_hint_dataset += (without_hint_message,)
        # sqlDF.show()

# print(hint_dataset)
schemaString = "id username hint_timestamp hint_content wrong_attempt answer first_correct_timestamp length"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaPeople = sqlContext.createDataFrame(hint_dataset, schema)
schemaPeople.createOrReplaceTempView("hint")


results = sqlContext.sql("SELECT * FROM hint where length between '0:00:00' and '0:05:00'")
print('0 - 5 ' + str(len(results.collect())) + str(len(results.collect()) / student_with_hint))
results = sqlContext.sql("SELECT * FROM hint where length between '0:05:01' and '0:10:00'")
print('5 - 10 ' + str(len(results.collect())) + str(len(results.collect()) / student_with_hint))
results = sqlContext.sql("SELECT * FROM hint where length between '0:10:01' and '0:20:00'")
print('10 - 20 ' + str(len(results.collect())) + str(len(results.collect()) / student_with_hint))
results = sqlContext.sql("SELECT * FROM hint where length between '0:20:01' and '0:30:00'")
print('20 - 30 ' + str(len(results.collect())) + str(len(results.collect()) / student_with_hint))
results = sqlContext.sql("SELECT * FROM hint where length between '0:30:01' and '0:59:59'")
print('30 - 60 ' + str(len(results.collect())) + str(len(results.collect()) / student_with_hint))
results = sqlContext.sql("SELECT * FROM hint where length between '0:00:00' and '0:59:59'")
print('0 - 60 ' + str(len(results.collect())) + str(len(results.collect()) / student_with_hint))
# results.write.json("5_2_1_hint.json")
results.show(100, False)

total_time = timedelta(hours=0, minutes=0, seconds=0)
number = 0
for tuple in results.collect():
    number += 1
    t = datetime.datetime.strptime(tuple[7], "%H:%M:%S")
    total_time += timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)

print(total_time.total_seconds() / number)
print(number)
print(student_with_hint)



schemaString = "id username first_wrong_timestamp wrong_attempt answer first_correct_timestamp length"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaPeople = sqlContext.createDataFrame(without_hint_dataset, schema)
schemaPeople.createOrReplaceTempView("without_hint")
# where length between '0:00:00' and '0:30:00

results = sqlContext.sql("SELECT * FROM without_hint where length between '0:00:00' and '0:05:00'")
print('0 - 5 ' + str(len(results.collect())) + str(len(results.collect()) / student_without_hint))
results = sqlContext.sql("SELECT * FROM without_hint where length between '0:05:01' and '0:10:00'")
print('5 - 10 ' + str(len(results.collect())) + str(len(results.collect()) / student_without_hint))
results = sqlContext.sql("SELECT * FROM without_hint where length between '0:10:01' and '0:20:00'")
print('10 - 20 ' + str(len(results.collect())) + str(len(results.collect()) / student_without_hint))
results = sqlContext.sql("SELECT * FROM without_hint where length between '0:20:01' and '0:30:00'")
print('20 - 30 ' + str(len(results.collect())) + str(len(results.collect()) / student_without_hint))
results = sqlContext.sql("SELECT * FROM without_hint where length between '0:30:01' and '0:59:59'")
print('30 - 60 ' + str(len(results.collect())) + str(len(results.collect()) / student_without_hint))
results = sqlContext.sql("SELECT * FROM without_hint where length between '0:00:00' and '0:59:59'")
print('0 - 60 ' + str(len(results.collect())) + str(len(results.collect()) / student_without_hint))

results = sqlContext.sql("SELECT * FROM without_hint where length between '0:00:00' and '0:59:59'")
results.show(100, False)
# results.write.json("5_2_1_no_hint.json")


total_time = timedelta(hours=0, minutes=0, seconds=0)
number2 = 0
for tuple in results.collect():
    number2 += 1
    t = datetime.datetime.strptime(tuple[6], "%H:%M:%S")
    total_time += timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    # if number2 == number:
    #     break

print(total_time.total_seconds() / number2)
print(number2)
print(student_without_hint)
# results_iteratable = results.collect()
# for line in results_iteratable:
#     print(line)

# results.show()
#
    # stringsDS = sqlDF.rdd.map(lambda row: "%s" % row.timestamp)
    # hint_timestamp = ''
    # for record in stringsDS.collect():
    #     hint_timestamp = record
    #
    # print('hint_timestamp' + hint_timestamp)


    # student_record_df.
    # print(student_record_df.show())


# df = sc.parallelize([
#     (list(k), ) + tuple(v[0])
#     for k, v in data_pkl['5', '2', '1'].items()
# ])
# hasattr(df, "toDF")
# print(df.toDF().show())

#
# pythonLines = lines.filter(lambda line: "Problem" in line)
# for line in pythonLines.take(10):
#     print(line)
# print(pythonLines.count())
# print(pythonLines.first())