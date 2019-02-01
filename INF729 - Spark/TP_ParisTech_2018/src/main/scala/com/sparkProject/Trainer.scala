package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object Trainer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.driver.maxResultSize" -> "2g"
    ))

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()

    import spark.implicits._

    /*******************************************************************************
      *
      *       TP 3
      *
      *       - lire le fichier sauvegarder précédemment
      *       - construire les Stages du pipeline, puis les assembler
      *       - trouver les meilleurs hyperparamètres pour l'entraînement du pipeline avec une grid-search
      *       - Sauvegarder le pipeline entraîné
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/

    // 1. Charger les données
    // Lecture des fichiers parquets et construction du DataFrame global
    var df_build: DataFrame = spark
      .read.parquet("./prepared_trainingset/part-00000-da4490f3-3af7-4783-a4a9-64355a59cb83-c000.snappy.parquet")

    for( i <- 1 to 7) {
      var path = "./prepared_trainingset/part-0000" + i + "-da4490f3-3af7-4783-a4a9-64355a59cb83-c000.snappy.parquet"
      var df_i = spark.read.parquet(path)
      df_build = df_build.unionAll(df_i)
    }

    val df = df_build
    // val df = spark.read.parquet("./prepared_trainingset/*")
    df.cache


    // 2. utiliser les données textuelles
    // Import des packages nécessaires
    import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover, IDF, StringIndexer, OneHotEncoder, VectorAssembler}

    // a. Découpage du texte en mots
    val tokenizer = new RegexTokenizer()
      .setPattern("\\W+")
      .setGaps(true)
      .setInputCol("text")
      .setOutputCol("words")

    // b. Retrait des Stop Words
    // Obtention de la liste des StopWords anglais
    val stopWordSet = StopWordsRemover.loadDefaultStopWords("english").toArray

    val remover = new StopWordsRemover()
      .setStopWords(stopWordSet)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered")

    //c. Comptage des mots : partie TF
    val vectorizer = new CountVectorizer()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("tf")

    //d. Partie IDF
    val idf = new IDF()
      .setInputCol(vectorizer.getOutputCol)
      .setOutputCol("tfidf")


    // 3. Convertir les catégories en numérique
    // e. Conversion la variable catégoricielle “country2” en quantités numériques.
    val indexer_country = new StringIndexer()
      .setInputCol("country2")
      .setOutputCol("country_indexed")

    // f. Conversion la variable catégoricielle “currency2” en quantités numériques.
    val indexer_currency = new StringIndexer()
      .setInputCol("currency2")
      .setOutputCol("currency_indexed")

    // g. Conversion des catégories country et currency en one_hot_encoder.
    val encoder_country = new OneHotEncoder()
      .setInputCol("country_indexed")
      .setOutputCol("country_onehot")

    val encoder_currency = new OneHotEncoder()
      .setInputCol("currency_indexed")
      .setOutputCol("currency_onehot")


    // 4. Mise en forme des données au format exploitable par SparkML
    // h. Assemblage des features en 1 colonne
    val assembler = new VectorAssembler()
      .setInputCols(Array("tfidf", "days_campaign", "hours_prepa", "goal", "country_onehot", "currency_onehot"))
      .setOutputCol("features")

    // i. Construction du modèle
    import org.apache.spark.ml.classification.LogisticRegression
    val lr = new LogisticRegression()
      .setElasticNetParam(0.0)
      .setFitIntercept(true)
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setStandardization(true)
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")
      .setThresholds(Array(0.7, 0.3))
      .setTol(1.0e-6)
      .setMaxIter(300)



    // j. Construction du pipeline
    import org.apache.spark.ml.Pipeline
    val stages_1 = Array(tokenizer, remover,  vectorizer, idf, indexer_country, indexer_currency, encoder_country, encoder_currency, assembler, lr)
    val pipeline_1 = new Pipeline().setStages(stages_1)


    // 5. Entrainement et tuning du modele

    // k. Split du df initial en train & test
    val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 12345)

    // Mise en cache des df
    training.cache
    test.cache

    // Définition de la grille de paramètres à tester
    import org.apache.spark.ml.tuning.ParamGridBuilder
    val paramGrid_1 = new ParamGridBuilder()
      .addGrid(vectorizer.minDF, Array(55.0, 75.0, 95.0))
      .addGrid(lr.regParam, Array(10e-8, 10e-6, 10e-4, 10e-2))
      .build()


    // Définition de la metric d'évaluation 'F1-score'
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val f1_evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("f1")
      .setLabelCol("final_status")
      .setPredictionCol("predictions")

    // Construction des modèles à calculer sur chaque paramètre de grille
    import org.apache.spark.ml.tuning.TrainValidationSplit
    val trainValidationSplit_1 = new TrainValidationSplit()
      .setEstimator(pipeline_1)
      .setEvaluator(f1_evaluator)
      .setEstimatorParamMaps(paramGrid_1)
      .setTrainRatio(0.7)

    // Calcul des modèles
    val tv_model_1 = trainValidationSplit_1.fit(training)

    // Récupération du meilleur modèle
    val best_model_1 = tv_model_1.bestModel

    // Calcul des prédictions du modèle obtenu sur le test set
    val df_WithPredictions_1 = best_model_1.transform(test)

    df_WithPredictions_1.cache

    // Calcul et affichage du F1-score sur le test set
    val f1 = f1_evaluator.evaluate(df_WithPredictions_1)
    println("F1 Score Lr = " + f1)

    // Affichage de la confusion matrix
    df_WithPredictions_1.groupBy("final_status", "predictions").count.show()


    //6. Test d'autres techniques de modélisation
    // A. Arbre de classification
    import org.apache.spark.ml.classification.DecisionTreeClassifier
    val decision_tree = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")

    val stages_2 = Array(tokenizer, remover,  vectorizer, idf, indexer_country, indexer_currency, encoder_country, encoder_currency, assembler, decision_tree)
    val pipeline_2 = new Pipeline().setStages(stages_2)

    val paramGrid_2 = new ParamGridBuilder()
      .addGrid(vectorizer.minDF, Array(55.0, 75.0, 95.0))
      .build()

    val trainValidationSplit_2 = new TrainValidationSplit()
      .setEstimator(pipeline_2)
      .setEvaluator(f1_evaluator)
      .setEstimatorParamMaps(paramGrid_2)
      .setTrainRatio(0.7)

    // B. Random Forest
    import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
    val random_forest = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")

    val stages_3 = Array(tokenizer, remover,  vectorizer, idf, indexer_country, indexer_currency, encoder_country, encoder_currency, assembler, random_forest)
    val pipeline_3 = new Pipeline().setStages(stages_3)

    val paramGrid_3 = new ParamGridBuilder()
      .addGrid(vectorizer.minDF, Array(55.0, 75.0, 95.0))
      .addGrid(random_forest.numTrees, Array(10, 1000, 100))
      .build()

    val trainValidationSplit_3 = new TrainValidationSplit()
      .setEstimator(pipeline_3)
      .setEvaluator(f1_evaluator)
      .setEstimatorParamMaps(paramGrid_3)
      .setTrainRatio(0.7)

    /*
    // Calcul des modèles
    val tv_model_2 = trainValidationSplit_2.fit(training)
    val tv_model_3 = trainValidationSplit_3.fit(training)

    // Récupération du meilleur modèle
    val best_model_2 = tv_model_2.bestModel
    val best_model_3 = tv_model_3.bestModel

    // Calcul des prédictions du modèle obtenu sur le test set
    val df_WithPredictions_2 = best_model_2.transform(test)
    val df_WithPredictions_3 = best_model_3.transform(test)

    df_WithPredictions_2.cache
    df_WithPredictions_3.cache

    // Calcul et affichage du F1-score sur le test set
    val f1_2 = f1_evaluator.evaluate(df_WithPredictions_2)
    println("F1 Score Arbre = " + f1_2)

    val f1_3 = f1_evaluator.evaluate(df_WithPredictions_3)
    println("F1 Score RandomForest = " + f1_3)

    // Affichage de la confusion matrix
    df_WithPredictions_2.groupBy("final_status", "predictions").count.show()
    df_WithPredictions_3.groupBy("final_status", "predictions").count.show()

    */

  }
}

