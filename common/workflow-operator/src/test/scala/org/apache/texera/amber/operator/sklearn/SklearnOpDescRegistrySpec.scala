/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.sklearn

import org.apache.texera.amber.operator.sklearn.training._
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Pins the wiring (Python import statement + user-friendly model name) for
  * every concrete `SklearnClassifierOpDesc` and `SklearnTrainingOpDesc`. A
  * typo in either string would silently misroute downstream UI labels and
  * breakage of the generated Python pipeline.
  */
class SklearnOpDescRegistrySpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Classifier registry (25 concrete SklearnClassifierOpDesc subclasses)
  // ---------------------------------------------------------------------------

  private val classifierEntries: List[(SklearnClassifierOpDesc, String, String)] = List(
    (
      new SklearnAdaptiveBoostingOpDesc(),
      "from sklearn.ensemble import AdaBoostClassifier",
      "Adaptive Boosting"
    ),
    (new SklearnBaggingOpDesc(), "from sklearn.ensemble import BaggingClassifier", "Bagging"),
    (
      new SklearnBernoulliNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import BernoulliNB",
      "Bernoulli Naive Bayes"
    ),
    (
      new SklearnComplementNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import ComplementNB",
      "Complement Naive Bayes"
    ),
    (
      new SklearnDummyClassifierOpDesc(),
      "from sklearn.dummy import DummyClassifier",
      "Dummy Classifier"
    ),
    (
      new SklearnDecisionTreeOpDesc(),
      "from sklearn.tree import DecisionTreeClassifier",
      "Decision Tree"
    ),
    (new SklearnExtraTreeOpDesc(), "from sklearn.tree import ExtraTreeClassifier", "Extra Tree"),
    (
      new SklearnExtraTreesOpDesc(),
      "from sklearn.ensemble import ExtraTreesClassifier",
      "Extra Trees"
    ),
    (
      new SklearnGaussianNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import GaussianNB",
      "Gaussian Naive Bayes"
    ),
    (
      new SklearnGradientBoostingOpDesc(),
      "from sklearn.ensemble import GradientBoostingClassifier",
      "Gradient Boosting"
    ),
    (
      new SklearnKNNOpDesc(),
      "from sklearn.neighbors import KNeighborsClassifier",
      "K-nearest Neighbors"
    ),
    (
      new SklearnLinearSVMOpDesc(),
      "from sklearn.svm import LinearSVC",
      "Linear Support Vector Machine"
    ),
    (
      new SklearnLogisticRegressionCVOpDesc(),
      "from sklearn.linear_model import LogisticRegressionCV",
      "Logistic Regression Cross Validation"
    ),
    (
      new SklearnLogisticRegressionOpDesc(),
      "from sklearn.linear_model import LogisticRegression",
      "Logistic Regression"
    ),
    (
      new SklearnMultiLayerPerceptronOpDesc(),
      "from sklearn.neural_network import MLPClassifier",
      "Multi-layer Perceptron"
    ),
    (
      new SklearnMultinomialNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import MultinomialNB",
      "Multinomial Naive Bayes"
    ),
    (
      new SklearnNearestCentroidOpDesc(),
      "from sklearn.neighbors import NearestCentroid",
      "Nearest Centroid"
    ),
    (
      new SklearnPassiveAggressiveOpDesc(),
      "from sklearn.linear_model import PassiveAggressiveClassifier",
      "Passive Aggressive"
    ),
    (
      new SklearnPerceptronOpDesc(),
      "from sklearn.linear_model import Perceptron",
      "Linear Perceptron"
    ),
    (
      new SklearnProbabilityCalibrationOpDesc(),
      "from sklearn.calibration import CalibratedClassifierCV",
      "Probability Calibration"
    ),
    (
      new SklearnRandomForestOpDesc(),
      "from sklearn.ensemble import RandomForestClassifier",
      "Random Forest"
    ),
    (
      new SklearnRidgeCVOpDesc(),
      "from sklearn.linear_model import RidgeClassifierCV",
      "Ridge Regression Cross Validation"
    ),
    (
      new SklearnRidgeOpDesc(),
      "from sklearn.linear_model import RidgeClassifier",
      "Ridge Regression"
    ),
    (
      new SklearnSDGOpDesc(),
      "from sklearn.linear_model import SGDClassifier",
      "Stochastic Gradient Descent"
    ),
    (new SklearnSVMOpDesc(), "from sklearn.svm import SVC", "Support Vector Machine")
  )

  classifierEntries.foreach {
    case (desc, expectedImport, expectedName) =>
      val cls = desc.getClass.getSimpleName
      cls should s"return import statement '$expectedImport'" in {
        assert(desc.getImportStatements == expectedImport)
      }
      it should s"return user-friendly model name '$expectedName'" in {
        assert(desc.getUserFriendlyModelName == expectedName)
      }
  }

  "SklearnClassifierOpDesc base class" should "default to empty strings before subclass overrides" in {
    val anonymous = new SklearnClassifierOpDesc {}
    assert(anonymous.getImportStatements == "")
    assert(anonymous.getUserFriendlyModelName == "")
  }

  it should "embed the import statement into generatePythonCode for a concrete subclass" in {
    val desc = new SklearnLogisticRegressionOpDesc()
    desc.target = "y"
    desc.countVectorizer = false
    // `tfidfTransformer` is a val on the base class, defaults to false.
    val code = desc.generatePythonCode()
    assert(code.contains("from sklearn.linear_model import LogisticRegression"))
    // Classifier OpDescs emit a UDFTableOperator pipeline.
    assert(code.contains("ProcessTableOperator"))
  }

  // ---------------------------------------------------------------------------
  // Training registry (26 concrete SklearnTrainingOpDesc subclasses)
  // ---------------------------------------------------------------------------

  private val trainingEntries: List[(SklearnTrainingOpDesc, String, String)] = List(
    (
      new SklearnTrainingAdaptiveBoostingOpDesc(),
      "from sklearn.ensemble import AdaBoostClassifier",
      "Training: Adaptive Boosting"
    ),
    (
      new SklearnTrainingBaggingOpDesc(),
      "from sklearn.ensemble import BaggingClassifier",
      "Training: Bagging"
    ),
    (
      new SklearnTrainingBernoulliNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import BernoulliNB",
      "Training: Bernoulli Naive Bayes"
    ),
    (
      new SklearnTrainingComplementNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import ComplementNB",
      "Training: Complement Naive Bayes"
    ),
    (
      new SklearnTrainingDecisionTreeOpDesc(),
      "from sklearn.tree import DecisionTreeClassifier",
      "Training: Decision Tree"
    ),
    (
      new SklearnTrainingDummyClassifierOpDesc(),
      "from sklearn.dummy import DummyClassifier",
      "Training: Dummy Classifier"
    ),
    (
      new SklearnTrainingExtraTreeOpDesc(),
      "from sklearn.tree import ExtraTreeClassifier",
      "Training: Extra Tree"
    ),
    (
      new SklearnTrainingExtraTreesOpDesc(),
      "from sklearn.ensemble import ExtraTreesClassifier",
      "Training: Extra Trees"
    ),
    (
      new SklearnTrainingGaussianNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import GaussianNB",
      "Training: Gaussian Naive Bayes"
    ),
    (
      new SklearnTrainingGradientBoostingOpDesc(),
      "from sklearn.ensemble import GradientBoostingClassifier",
      "Training: Gradient Boosting"
    ),
    (
      new SklearnTrainingKNNOpDesc(),
      "from sklearn.neighbors import KNeighborsClassifier",
      "Training: K-nearest Neighbors"
    ),
    (
      new SklearnTrainingLinearSVMOpDesc(),
      "from sklearn.svm import LinearSVC",
      "Training: Linear Support Vector Machine"
    ),
    (
      new SklearnTrainingLogisticRegressionCVOpDesc(),
      "from sklearn.linear_model import LogisticRegressionCV",
      "Training: Logistic Regression Cross Validation"
    ),
    (
      new SklearnTrainingLogisticRegressionOpDesc(),
      "from sklearn.linear_model import LogisticRegression",
      "Training: Logistic Regression"
    ),
    (
      new SklearnTrainingMultiLayerPerceptronOpDesc(),
      "from sklearn.neural_network import MLPClassifier",
      "Training: Multi-layer Perceptron"
    ),
    (
      new SklearnTrainingMultinomialNaiveBayesOpDesc(),
      "from sklearn.naive_bayes import MultinomialNB",
      "Training: Multinomial Naive Bayes"
    ),
    (
      new SklearnTrainingNearestCentroidOpDesc(),
      "from sklearn.neighbors import NearestCentroid",
      "Training: Nearest Centroid"
    ),
    (
      new SklearnTrainingPassiveAggressiveOpDesc(),
      "from sklearn.linear_model import PassiveAggressiveClassifier",
      "Training: Passive Aggressive"
    ),
    (
      new SklearnTrainingPerceptronOpDesc(),
      "from sklearn.linear_model import Perceptron",
      "Training: Linear Perceptron"
    ),
    (
      new SklearnTrainingProbabilityCalibrationOpDesc(),
      "from sklearn.calibration import CalibratedClassifierCV",
      "Training: Probability Calibration"
    ),
    (
      new SklearnTrainingRandomForestOpDesc(),
      "from sklearn.ensemble import RandomForestClassifier",
      "Training: Random Forest"
    ),
    (
      new SklearnTrainingRidgeCVOpDesc(),
      "from sklearn.linear_model import RidgeClassifierCV",
      "Training: Ridge Regression Cross Validation"
    ),
    (
      new SklearnTrainingRidgeOpDesc(),
      "from sklearn.linear_model import RidgeClassifier",
      "Training: Ridge Regression"
    ),
    (
      new SklearnTrainingSDGOpDesc(),
      "from sklearn.linear_model import SGDClassifier",
      "Training: Stochastic Gradient Descent"
    ),
    (
      new SklearnTrainingSVMOpDesc(),
      "from sklearn.svm import SVC",
      "Training: Support Vector Machine"
    ),
    (
      new SklearnTrainingLinearRegressionOpDesc(),
      "from sklearn.linear_model import LinearRegression",
      "Training: Linear Regression"
    )
  )

  trainingEntries.foreach {
    case (desc, expectedImport, expectedName) =>
      val cls = desc.getClass.getSimpleName
      cls should s"return import statement '$expectedImport'" in {
        assert(desc.getImportStatements == expectedImport)
      }
      it should s"return user-friendly model name '$expectedName'" in {
        assert(desc.getUserFriendlyModelName == expectedName)
      }
  }

  "SklearnTrainingOpDesc default" should "use the RandomForest defaults until a subclass overrides" in {
    val base = new SklearnTrainingOpDesc()
    assert(base.getImportStatements == "from sklearn.ensemble import RandomForestClassifier")
    assert(base.getUserFriendlyModelName == "RandomForest Training")
  }

  it should "embed the import statement into generatePythonCode for a concrete subclass" in {
    val desc = new SklearnTrainingLogisticRegressionOpDesc()
    desc.target = "y"
    desc.countVectorizer = false
    desc.tfidfTransformer = false
    val code = desc.generatePythonCode()
    assert(code.contains("from sklearn.linear_model import LogisticRegression"))
    assert(code.contains("ProcessTableOperator"))
  }
}
