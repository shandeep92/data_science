# Project 4: West Nile Virus Prediction

![](https://github.com/shandeep92/dsi14projects/blob/master/project4/plot_images/lineplot_wnvpresent_over_months.png)

## Context 


West-Nile-Virus(WNV) is a mosquito-borne disease that has plagued the continental United States since 1999. The vast majority of infected people will develop mild symptoms that will subside over a few days to several weeks (source). About 1 out of 150 infected people develop a serious illness. The sometimes neuroinvasive virus may cause encephalitis and meningitis, which can prove to be fatal. There is currently no vaccine to prevent or medication to treat WNV (source).

Outbreaks typically intensify over as little as a couple of weeks; however, human case reports are lagging indicators of risk since case reports occur weeks after the time of infection. Thus, environmental surveillance – monitoring enzootic and epizootic WNV transmission in mosquitoes and birds – forms a timelier index of risk, and is an important cornerstone for implementing effective WNV risk reduction efforts.Research and operational experience shows that increases in WNV infection rates in mosquito populations can provide an indicator of developing outbreak conditions several weeks in advance of increases in human infections. Aggressive and timely efforts to reduce the number of infected adult mosquitoes will optimally impact human WNV case incidence (source)



## Scope

The goal of this project is to derive a plan to deploy pesticides throughout the city of Chicago. Hence, the scope of our plan for deployment and cost analysis will also be limited within the city of Chicago.

However, the model that we have trained can certainly be used in other states/cities within the United States to predict the potential outbreak of West Nile Virus.

The modelling techniques and strategies will be presented to biostaticians, epidemiologists, American Public Health Service and decision makers from Centers for Disease Control and Prevention (CDC).


## Problem Statement


1. To predict locations with high potential of having mosquitoes carrying West Nile Virus.
2. Find out factors that contribute to the growth and spread of the virus in mosquitoes.


---

## Files Directory

**1. input**
- [Kaggle Train Dataset](./input/train.csv)
- [Kaggle Test Dataset](./input/test.csv)
- [Kaggle Spray Dataset](./input/spray.csv)
- [Kaggle Weather Dataset](./input/weather.csv)
- [Kaggle Map Data](./input/mapdata_copyright_openstreetmap_contributors.txt)
- [Cleaned Train Dataset](./input/cleaned_train.csv)
- [Cleaned Test Dataset](./input/cleaned_test.csv)
- [Cleaned Spray Dataset](./input/cleaned_spray.csv)
- [Cleaned Weather Dataset](./input/cleaned_weather.csv)
- [Merged Train Dataset](./input/merged_train_lag_10.csv)
- [Merged Test Dataset](./input/merged_test_lag_10.csv)

**2. code**
- [Data Cleaning](./code/1_data_cleaning.ipynb)
- [Feature Engineering ](./code/2_merging_data_feature_engineering.ipynb)
- [EDA, Modelling & Evaluation ](./code/3_EDA_modelling_evaluation.ipynb)

**3. kaggle_submission**  

Contains our kaggle attempts

**4. deployment**
- [Code](./deployment/model.py)
- [Model in pickled file](./deployment/final_rfc_model.pkl)
- [Templates](./deployment/templates/form_upload.html)

**5. plot_images**  

Contains all the relevant images that we have in the notebooks



## Data Dictionary

### Variables
|Feature|Type|Description|
|---|---|---|
|Date|object|Date the test is performed|
|Latitude|float64|Latitude returned from GeoCoder|
|Longitude|float64|Longitude returned from GeoCoder|
|Species_CULEX PIPIENS|int64|Mosquito species CULEX PIPIENS|
|Species_CULEX PIPIENS/RESTUANS|int64|Mosquito species CULEX PIPIENS/RESTUANS|
|Species_CULEX RESTUANS|int64|Mosquito species CULEX RESTUANS|
|WnvPresent|int64|whether West Nile Virus was present in these mosquitos. 1 means WNV is present, and 0 means not present|
|Sprayed|int64|Spraying occured 7 days prior to the testing date and it occured within 100m of a mosquito trap|
|Avg Pos Dist|float64|Average distance of previously positively tested mosquito traps from each individual trap being tested within 3.2km|
|Tmax|float64|Max temperature|
|Tmin|float64|Min temperature|
|Tavg|float64|Average of max and min temperature|
|DewPoint|float64|Average dewpoint temperature|
|WetBulb|float64|Average wetbulb temperature|
|Heat|float64|Heating degree (SEASON BEGINS WITH JULY)|
|Cool|float64|Cooling degree (SEASON BEGINS WITH JANUARY)|
|PrecipTotal|float64|rainfall in inches (24-HR PERIOD ENDING AT INDICATED LOCAL STANDARD TIME)|
|StnPressure|float64|Average Station Pressure (INCHES OF HG)|
|SeaLevel|float64|Average Sea level Pressure (INCHES OF HG)|
|ResultSpeed|float64|Resultant Wind Speed (MILES PER HOUR)|
|ResultDir|float64|Resultant Wind Direction (WHOLE DEGREES)|
|AvgSpeed|float64|Average Wind Speed (MILES PER HOUR)|
|Sunrise|float64|Time of Sunrise|
|Sunset|float64|Time of Sunset|
|bc|int64|Code of Weather Phenomena PATCHES|
|br|int64|Code of Weather Phenomena MIST|
|dz|int64|Code of Weather Phenomena DRIZZLE|
|fg|int64|Code of Weather Phenomena FOG|
|fg+|int64|Code of Weather Phenomena HEAVY FOG|
|fu|int64|Code of Weather Phenomena SMOKE|
|gr|int64|Code of Weather Phenomena HAIL|
|hz|int64|Code of Weather Phenomena HAZE|
|mi|int64|Code of Weather Phenomena SHALLOW|
|ra|int64|Code of Weather Phenomena RAIN|
|sn|int64|Code of Weather Phenomena SNOW|
|sq|int64|Code of Weather Phenomena SQUALL|
|ts|int64|Code of Weather Phenomena THUNDERSTORM|
|vc|int64|Code of Weather Phenomena VICINITY|

---

## Deployment of Model

We have also deployed our model so that it can be used by the relevant departments such as the operations team to identify the locations that require spraying. This can also be used in other cities/states. The engine works such that anyone can upload a CSV file with the features that are needed for predictions and a table indicating the presence of West Nile Virus will be generated.

However, the main limitation is that the model was deployed on our local server using a *flask* application. We can look into improving it by deploying it to cloud. We can also improve the frontend interface so that it can provide more useful information and visualizations.



## Conclusions and Recommendations


We have selected the *RandomForestClassifier* with hyperparameter *class_weight='balanced_subsample'* as our model of choice, with what we deemed was the best balance between *accuracy* and *sensitivity* (due to the nature of our prediction being disease related and the minimisation of *false negatives* are imperative). *RandomForestClassifier* performed the best in these two metrics when compared to *LogisticRegression, GradientBoosting* and *SVC* models. *RandomForestClassifier* also performed relatively well with *SMOTE* as our balancing technique, however comparing the significant increase in *sensitivity*, we felt that a slight decrease in *accuracy* using *class_weight='balanced_subsample'* was justified. Other balancing techniques such as *ADASYN* and *ClusterCentroids* were considered as well. However, *ADASYN* was inferior to our selection and *ClusterCentroids* is an undersampling technique that brought the negative class down to a number that would be detrimental to the model performance.

The model was created based on the data of 8304 readings in Chicago between 2007 and 2013. The applicability of this model can be brought over and applied to other states across the United States as it generalizes quite well as was seen in our test sample.

There are a few limitations to our model and our data. Due to the imbalanced nature of our positive and negative classes, we have had to use balancing techniques which are weightage inflation and oversampling methods. Acquiring more data would be better in reducing this phenomena and aid in generating a more reliable model.

The hyperparameters involved in the instantiation of our model were based on a trial and error method in maximising both *accuracy* and *sensitivity*, as *GridSearchCV* prioritises a singular metric and not a balance of two. There may be a combination of hyperparameter settings that could have done better and could have been missed out during the trial and error phase, as there are too many variations of such combinations to have been covered.

The features involved that took the predominant influence on our models were *Average Positive Distance, Sunrise, Sunset* and *DewPoint*. *Average Positive Distance* was a newly engineered feature based on the average distance of previously positively tested mosquito traps from each individual trap being tested. These are factors to be looked into when considering the presence of West Nile Virus in a particular location. Due to the nature of our features being beyond what is humanly controllable, mitigation efforts can be only limited to methods such as spraying - which would be done in locations churned within our model based on the aforementioned features.

**Mitigation efforts to curb the spread of West Nile Virus should not be the responsibility of the state alone, especially when considering households and their individual influences on mosquito population.** The state could further educate the population through campaigns and programmes on measures and steps to take at home or within their communities. Integrated vector management programs should be carried out and the severity of the disease should be communicated to the public in a manner to illicit preventive action without reliance on statutory intervention.
