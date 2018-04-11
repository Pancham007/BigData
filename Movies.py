# Databricks notebook source
ratings = spark.sql("SELECT * FROM ratings")

# COMMAND ----------

movies = spark.sql("SELECT * FROM movies")

# COMMAND ----------

tags=spark.sql("SELECT * FROM tags")

# COMMAND ----------

avgRatings=spark.sql("SELECT movieId,avg(rating) from ratings group by movieId")

# COMMAND ----------

display(avgRatings)

# COMMAND ----------

avgRatings = avgRatings.withColumnRenamed( "avg(CAST(rating AS DOUBLE))","avgRating")

# COMMAND ----------

rawratings = avgRatings.join(movies, 'movieId').select( avgRatings.movieId,movies.title,avgRatings.avgRating)

# COMMAND ----------

display(rawratings)

# COMMAND ----------

bottomten=spark.createDataFrame(avgRatings.orderBy(avgRatings.avgRating.asc()).take(10))
display(bottomten)

# COMMAND ----------

worstrating= bottomten.join(movies, 'movieId').select(bottomten.movieId,movies.title,bottomten.avgRating)

# COMMAND ----------

display(worstrating)

# COMMAND ----------

actionmovie=tags.filter(tags.tag == 'action').join(ratings,"movieId").groupby(tags.movieId).agg({"rating": "avg"})

# COMMAND ----------

actionratings=movies.join(actionmovie,"movieId")

# COMMAND ----------

actionratings = actionratings.withColumnRenamed("avg(rating)","avg_Rating").orderBy('avg_Rating', ascending=False)

# COMMAND ----------

display(actionratings)

# COMMAND ----------

actionmovies=tags.filter(tags.tag == 'action')
thriller=movies.filter("genres like '%Thriller%'")

# COMMAND ----------

thrillplusaction=actionmovies.join(thriller,'movieId')

# COMMAND ----------

display(thrillplusaction)

# COMMAND ----------

avg_final=thrillplusaction.join(ratings,'movieId').groupby(tags.movieId).agg({"rating": "avg"})

# COMMAND ----------

thrillactionrating=movies.join(avg_final,"movieId")

# COMMAND ----------

display(thrillactionrating)
