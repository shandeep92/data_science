# Capstone Project: Real/Fake News Prediction

![](https://github.com/shandeep92/capstone/blob/master/wordcloud_keywords_topics%20copy.jpg)

## Context 


Fake news has risen dramatically in popular consciousness over the last few years. According to a Pew Research Center study, Americans deem fake news to be a larger problem than racism, climate change or terrorism. With the advent of social media and the amount of information accessible to us, it is getting increasingly difficult to distinguish between real news and fake news. Therefore, this could have severe repercussions within society if the problem is not dealt with.



## Scope

The goal of this project was to come up with a model that distinguishes real and fake news using a kaggle data set that contains real news and fake news based on US news outlets. In order to tackle the problem of fake news, several classification models such as the Logistic Regression and Decision trees were applied. It is worth pointing out that the data set is limited in terms of its time frame where news was collated between 2015 and 2018 and also only based on US news outlets. However, the model that has been trained can certainly be helpful in other countries as well.


## Problem Statement

To predict real/fake news based on a kaggle dataset containing various news sources within the US.


---

## Data Dictionary

### Variables
|Feature|Type|Dataset|Description|
|---|---|---|---|
|title|object|true/fake kaggle|title of news article| 
|text|object|true/fake kaggle|content of the news|
|category|int|true/fake kaggle|"1" = true, "0" = fake (target variable)|
|title_length|int|true/fake kaggle|number of words in title of news article| 
|text_length|int|true/fake kaggle|number of words in content of news|

---


## Conclusions and Recommendations


I have selected the *Decision Trees Classifier* with hyperparameter *CountVectorization* and *TfidTransformer* as the model of choice based on classification metrics such as sensitivity, specificity and precision along with the accuracy of the results.


The model was then deployed on unseen data but did not perform as well as it did on the validation set. The data set leaned very heavily towards US news and they were from a small sample of news sources such as CNN. Therefore, it makes sense that the model didn't perform well when the BBC or Al Jazeera was used as unseen data. Another factor to consider is that the time period was very short (2015-2018) and typically, news content is dynamic in nature. Suppose if an article covers covid-19 related article today, it is very likely that the model would predict it as fake news. Therefore, one has to be aware of its limitations. However, that being said, this approach could prove to be useful if a much larger corpus of various news outlets and an extended time frame is factored into the equation.

For future research, one could also explore deep learning techniques such as word2vec which could provide better results.
