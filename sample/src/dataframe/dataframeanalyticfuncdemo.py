from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *


# Window aggregate functions (aka window functions or windowed aggregates) are functions that perform a calculation
# over a group of records called window that are in some relation to the current record (i.e. can be in the same
# partition or frame as the current row).
# # In other words, when executed, a window function computes a value for each and every row in a window
# (per window specification).
# The main difference between window aggregate functions and aggregate functions with grouping operators is that the
# former calculate values for every row in a window while the latter gives you at most the number of input rows,
# one value per group.

# create spark session
spark = SparkSession.builder.master("local[*]").appName('explore analytical functions').getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', 2)

# load order_items file in dataframe
orderItems = spark.read.format('csv').\
    schema('order_item_id int, order_item_order_id int, order_item_product_id int, order_item_quantity int, '
           'order_item_subtotal float, order_item_product_price float').\
    load("file:/home/vagrant/Downloads/data-master/retail_db/order_items")

# creating window  on order_item_order_id
spec = Window.partitionBy(orderItems.order_item_order_id)

# create df with column revenue for every unique order_id by summing order_item_subtotal & round it to 2 decimal places
orderItemsWithRevenue = orderItems.withColumn('revenue', round(sum("order_item_subtotal").over(spec), 2))
orderItemsWithRevenue.show()


# load employee data from hr_db
employees = spark.read.format('csv').\
    option('sep','\t').\
    schema('''employee_id INT, 
            first_name STRING, 
            last_name STRING, 
            email STRING,
            phone_number STRING, 
            hire_date STRING, 
            job_id STRING, 
            salary FLOAT,
            commission_pct STRING,
            manager_id STRING, 
            department_id STRING
         '''). \
  load("file:/home/vagrant/Downloads/data-master/hr_db/employees")

# employees.show()

# create window over department_id and sort the result in descending order of salary
spec1 = Window.partitionBy(employees.department_id).orderBy(employees.salary.desc())

# calculate total salary by department, and also the min, max and average salary in department
employees.select('employee_id', 'department_id', 'salary').\
    withColumn('salary_expense', round(sum("salary").over(spec1), 2)).\
    withColumn('minimum dept salary', min("salary").over(spec1)).\
    withColumn('maximum dept salary', max("salary").over(spec1)).\
    withColumn('average dept salary', avg("salary").over(spec1)).\
    sort('department_id').\
    show()

# lead gives the next highest salary compared to current row. Here 1 passed as argument in lead goes for next highest.
# If 2 is passed, then it will give second highest salary compared to current row.

# '''
# SELECT employee_id, salary, department_id,
#   lead(salary, 1) OVER (PARTITION BY department_id ORDER BY salary DESC) lead_salary
# FROM employees
# ORDER BY department_id, salary DESC;
# '''

employeesLead = employees. \
  select('employee_id', 'salary', 'department_id'). \
  withColumn('lead_salary', lead(employees.salary, 1).over(spec1)). \
  orderBy(employees.department_id, employees.salary.desc())

employeesLead.show()

# lag gives the next loweest salary as compared to current row
# SELECT employee_id, salary, department_id,
#   lag(salary) OVER (PARTITION BY department_id ORDER BY salary DESC) lag_salary
# FROM employees
# ORDER BY department_id, salary DESC;

employeesLag = employees. \
  select('employee_id', 'salary', 'department_id'). \
  withColumn('lag_salary', lag(employees.salary, 1).over(spec1)). \
  orderBy(employees.department_id, employees.salary.desc())

employeesLag.show()

# first returns the first value in group
employeesFirst = employees. \
  select('employee_id', 'salary', 'department_id'). \
  withColumn('first_salary', first(employees.salary).over(spec1)). \
  orderBy(employees.department_id, employees.salary.desc())

employeesFirst.show()

# last returns the last value in group
# SELECT employee_id, salary, department_id,
#   last_value(salary) OVER
#     (PARTITION BY department_id ORDER BY salary DESC
#      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_salary
# FROM employees
# ORDER BY department_id, salary DESC;

spec2 = Window. \
  partitionBy('department_id'). \
  orderBy(employees.salary.desc()). \
  rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

employeesLast = employees. \
  select('employee_id', 'salary', 'department_id'). \
  withColumn('last_salary', last(employees.salary).over(spec2)). \
  orderBy(employees.department_id, employees.salary.desc())

employeesLast.show()

# rank gives the sparse rank i.e. if there are duplicates, then it will assign same rank to all duplicates and
# next rank will be previous rank + no. of duplicates For ex, if there are four 17th rank, then next rank is 21.
# dense_rank does normal ranking i.e. if there are duplicates, then it assigns next only.
employeesRank = employees. \
    select('employee_id', 'salary', 'department_id'). \
    withColumn('Rank', rank().over(spec1)). \
    withColumn('DenseRank', dense_rank().over(spec1)). \
    withColumn('RowNumber',row_number().over(spec1)).\
    orderBy(employees.department_id, employees.salary.desc())

employeesRank.show(100)


