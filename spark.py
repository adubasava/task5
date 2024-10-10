import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, count, sum, when, round, expr
from functools import reduce

spark = SparkSession.builder \
    .appName("task5") \
    .config("spark.jars", "./postgresql-42.7.4.jar") \
    .getOrCreate()

JDBC_URL = "jdbc:postgresql://localhost:5432/pagila"
connection_properties = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver"
}

def load_table(table_name):
    return spark.read.jdbc(
    url=JDBC_URL,
    table=table_name,
    properties=connection_properties
)

tables = ["actor", "film_actor", "film", "customer", "rental", \
          "film_category", "category", "inventory", "city", "address"]
dataframes = {table: load_table(table) for table in tables}

# Количество фильмов в каждой категории, по убыванию
category_df = dataframes["category"]
film_category_df = dataframes["film_category"]
film_df = dataframes["film"]

result_df = (
    category_df.alias("c")
    .join(film_category_df.alias("fc"), col("c.category_id") == col("fc.category_id"))
    .join(film_df.alias("f"), col("fc.film_id") == col("f.film_id"))
    .groupBy(col("c.name").alias("category"))
    .agg(count(col("f.film_id")).alias("film_count"))
    .orderBy(col("film_count").desc())
)

result_df.show()

# 10 актеров, чьи фильмы больше всего арендовали, по убыванию
actor_df = dataframes["actor"]
film_actor_df = dataframes["film_actor"]
inventory_df = dataframes["inventory"]
rental_df = dataframes["rental"]

result2_df = (
    actor_df.alias("a")
    .join(film_actor_df.alias("fa"), col("a.actor_id") == col("fa.actor_id"))
    .join(inventory_df.alias("i"), col("fa.film_id") == col("i.film_id"))
    .join(rental_df.alias("r"), col("i.inventory_id") == col("r.inventory_id"))
    .groupBy(col("a.actor_id"), 
             concat_ws(" ", col("a.first_name"), col("a.last_name")).alias("actor"))
    .agg(count(col("r.rental_id")).alias("rental_count"))
    .orderBy(col("rental_count").desc())
    .limit(10)
)

result2_df.show()

payment_tables = ["payment_p2022_01", "payment_p2022_02", "payment_p2022_03",\
                  "payment_p2022_04", "payment_p2022_05", "payment_p2022_06", "payment_p2022_07"]

payment_dfs = [load_table(table) for table in payment_tables]

all_payments_df = reduce(lambda df1, df2: df1.union(df2), payment_dfs)

# Категория фильмов, на которую потратили больше всего денег
actor_df = dataframes["actor"]
film_actor_df = dataframes["film_actor"]
inventory_df = dataframes["inventory"]
rental_df = dataframes["rental"]

result3_df = (
    category_df.alias("c")
    .join(film_category_df.alias("fc"), col("c.category_id") == col("fc.category_id"))
    .join(film_df.alias("f"), col("fc.film_id") == col("f.film_id"))
    .join(inventory_df.alias("i"), col("f.film_id") == col("i.film_id"))
    .join(rental_df.alias("r"), col("i.inventory_id") == col("r.inventory_id"))
    .join(all_payments_df.alias("p"), col("r.rental_id") == col("p.rental_id"))
    .groupBy(col("c.name").alias("category_name"))
    .agg(sum(col("p.amount")).alias("total_spent"))
    .orderBy(col("total_spent").desc())
    .limit(1)
)

result3_df.show()

# Названия фильмов, которых нет в inventory
result4_df = (
    film_df.alias("f")
    .join(inventory_df.alias("i"), col("f.film_id") == col("i.film_id"), "left")
    .filter(col("i.film_id").isNull())
    .select("f.title").distinct()
)

result4_df.show()

# Топ 3 актеров, которые больше всего появлялись в фильмах в категории Children. 
# Если у нескольких актеров одинаковое количество фильмов, вывести всех

# Актеры в фильмах категории Children
children_films_df = (
    actor_df.alias("a")
    .join(film_actor_df.alias("fa"), col("a.actor_id") == col("fa.actor_id"))
    .join(film_df.alias("f"), col("fa.film_id") == col("f.film_id"))
    .join(film_category_df.alias("fc"), col("f.film_id") == col("fc.film_id"))
    .join(category_df.alias("c"), col("c.category_id") == col("fc.category_id"))
    .filter(col("c.name") == "Children")
    .groupBy(col("a.actor_id"), 
             concat_ws(" ", col("a.first_name"), col("a.last_name")).alias("actor"))
    .agg(count(col("f.film_id")).alias("film_count"))
)

# Пороговое значение для количества фильмов (третье с конца)
threshold_df = (
    children_films_df.select("film_count")
    .distinct()
    .orderBy(col("film_count").desc())
    .limit(3)
)

threshold_value = threshold_df.collect()[-1]["film_count"]

result5_df = (
    children_films_df
    .filter(col("film_count") >= threshold_value)
    .orderBy(col("film_count").desc(), col("actor"))
)

result5_df.show()

# Города с количеством активных и неактивных клиентов; по количеству неактивных клиентов по убыванию
city_df = dataframes["city"]
address_df = dataframes["address"]
customer_df = dataframes["customer"]

result6_df = (
    city_df.alias("c")
    .join(address_df.alias("a"), col("a.city_id") == col("c.city_id"))
    .join(customer_df.alias("cust"), col("cust.address_id") == col("a.address_id"))
    .groupBy(col("c.city").alias("city"))
    .agg(
        sum(when(col("cust.active") == 1, 1).otherwise(0)).alias("active_customers"),
        sum(when(col("cust.active") == 0, 1).otherwise(0)).alias("inactive_customers")
    )
    .orderBy(col("inactive_customers").desc())
)

result6_df.show()

# Категория фильмов, у которой самое большое количество часов суммарной аренды в городах, 
# которые начинаются на букву "а". То же самое для городов, в которых есть символ "-"

rental_hours_df = (
    rental_df.alias("r")
    .join(inventory_df.alias("i"), col("r.inventory_id") == col("i.inventory_id"))
    .join(film_df.alias("f"), col("i.film_id") == col("f.film_id"))
    .join(film_category_df.alias("fc"), col("f.film_id") == col("fc.film_id"))
    .join(category_df.alias("cat"), col("cat.category_id") == col("fc.category_id"))
    .join(customer_df.alias("cust"), col("r.customer_id") == col("cust.customer_id"))
    .join(address_df.alias("addr"), col("cust.address_id") == col("addr.address_id"))
    .join(city_df.alias("c"), col("addr.city_id") == col("c.city_id"))
    .groupBy(col("c.city"), col("cat.name").alias("category"))
    .agg(round(sum(expr("unix_timestamp(r.return_date) - unix_timestamp(r.rental_date)")
                    / 3600), 2).alias("total_hours"))
)

rental_hours_df.show()

# City A
city_a_df = (
  rental_hours_df
  .filter(col("city").like("a%"))
  .groupBy(col("category"))
  .agg(sum(col("total_hours")).alias("total_hours"))
  .orderBy(col("total_hours").desc())
  .limit(1)
)

# City dash
city_dash_df = (
  rental_hours_df
  .filter(col("city").like("%-%"))
  .groupBy(col("category"))
  .agg(sum(col("total_hours")).alias("total_hours"))
  .orderBy(col("total_hours").desc())
  .limit(1)
)

result7_df = (
    city_a_df.withColumn("city_type", expr("'Cities starting with A'"))
    .unionAll(
        (city_dash_df.withColumn("city_type", expr("'Cities with - dash'")))
    )
)

result7_df.show()
