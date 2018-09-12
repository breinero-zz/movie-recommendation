import com.mongodb.spark._

val ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "recommendation").option("collection", "ratings").load()
case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

import spark.implicits._
val ratingsDS = ratings.as[Rating]
ratingsDS.cache()
ratingsDS.show()

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

/* import org.apache.spark.ml.recommendation.ALS.Rating */
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
val model = als.fit(training)

// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop")
val predictions = model.transform(test)

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")

// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)
// Generate top 10 user recommendations for each movie
val movieRecs = model.recommendForAllItems(10)

val df1 = userRecs.select($"userId", explode($"recommendations")).toDF( "userId", "recommendation")
val df2 = df1.select( $"userId", col("recommendation")("movieId").as("movieId"), col("recommendation")("rating").as("rating") )
val df3 = df2.map( r => ( r.getInt(0), r.getInt(1), r.getFloat(2).toDouble ) ).toDF( "userID", "movieId", "rating" )
df3.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("collection", "perUser").save()

