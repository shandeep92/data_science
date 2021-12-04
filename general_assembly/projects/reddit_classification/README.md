# Project 3: Classifying Subreddits

![](https://github.com/shandeep92/dsi14projects/blob/master/project3/images/wordcloud.png)

### Problem Statement
This project analyses the language used in the Singapore and UK subreddits and the aim was to see if the language used in these subreddits were unique to both Singapore and the UK.


### Executive Summary
In this project, I focused on two particular subreddits [r/singapore](https://www.subreddit.com/r/singapore) and [r/unitedkingdom](https://www.subreddit.com/r/unitedkingdom). The aim of this project was to analyse the language used in these subreddits and to examine whether or not they were unique to both Singapore and the United Kingdom. In order to tackle this problem, classification models such as the Naive Bayes and Logistic Regression were applied.

### Findings and Conclusion

Logistic Regression performed much better than the Naive Bayes model with an accuracy above 75% for both the CountVectorization and the TFIDVectorization. As baseline model was around 50% for both classes, this represents a significant improvement. However, having said that, the model was still overfitted. The model that produced the best result is GridSearch using TFIDVectorizer and Logistic Regression.

Despite the limitations of overfitting, the model still has some value. This approach could be
used for other applications in NLP. In terms of of similarities between two countries, one could point to the fact that both the UK and Singapore are going through the Covid 19 period so a lot of the language used in these posts will be quite similar (even after excluding for stopwords) or perhaps day to day discussions do not vary as much between countries. They might differ in terms of their politics or current affairs in their respective countries.

In order to improve the accuracy of the model, a larger corpus which incorporates a much wider range of vocabulary could prove to be helpful.

### Data Dictionary

|Feature|Type|Dataset|Description|
|---|---|---|---|
|body|object|sg/uk subreddits|text of the comments| 
|created_utc|int|sg/uk subreddits|timestamp of the comments based on EPOCH time format|
|id|object|sg/uk subreddits|unique id of each reddit post|
|parent_id|object|sg/uk subreddits|unique id of parent comment(aka first tier comments)|
|score|int|sg/uk subreddits|the number of upvotes for the comments|
|subreddit|obj|sg/uk subreddits|r/singapore and r/unitedkingdom subreddits (target variable)| 
|word_length|int|sg/uk subreddits|length of words in the comments|

### Datasets

- [singapore subreddit](https://www.subreddit.com/r/singapore)
- [uk subreddit](https://www.subreddit.com/r/unitedkingdom)

