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
val docs  = predictions.map( r => ( r.getInt(4), r.getInt(1),  r.getDouble(2) ) ).toDF( "userID", "movieId", "rating" )
docs.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("collection", "recommendations").save()