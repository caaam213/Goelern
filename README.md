# Goelern

## Author 
MERAOUI Camelia

## Description
Initially, the goal is to create an app to learn German using data from https://fichesvocabulaire.com/. 
Then, those data will be used to create a web interface to learn German. 

## Installation and running the project
1. Clone the repository using the git bash
```
    git clone git@github.com:caaam213/Goelern.git
```

2. Build the two images from the [Folder app Dockerfile](/app/Dockerfile) and [Folder webapp Dockerfile](/webapp/Dockerfile) using those commands : 
```
    docker build --pull --rm -f "app\Dockerfile" -t goelern-etl:latest "app"
```


```
    docker build --pull --rm -f "webapp\Dockerfile" -t webapp:latest "webapp"
```

3. Rename docker-compose-default.yaml to docker-compose.yaml and add amazonS3 Credentials

4. Execute the docker-compose file in order to activate the needed images. 

The images are : 
    
- goelern-etl : It is the image which enables the data collection using airflow
- webapp : Image to launch the webapp 
- mongo : Image to use MongoDB to store and interact with data

4. Connect to Airflow interface : http://localhost:8080/ with Admin as user and the password from this [file](/app/airflow/standalone_admin_password.txt)

5. Navigate on Admin/Connections and create a user with those information : 
![mongo_airflow](/readme_images/mongo_airflow.png)

6. You can now use the airflow interface to execute the DAG :)

## Architecture
### App folder 
This folder contains the pipeline to get data from https://fichesvocabulaire.com/ using Airflow.

**create-parameters DAG**

The goal is to create parameters for the goelern-etl DAG. 
The parameters are the targeted language and the URL.
This pipeline will be composed of : 
- reinitialise status : Reinitialise the status of the parameter each month so the parameter will be scrapable again (not implemented yet)
- crawling_voc_list_urls : Class which contains functions to get urls, create parameters and store them.

**goelern-etl DAG**

This DAG is a pipeline which uses those functions for now :
- get_parameters : Not created yet. The goal is to select a parameter created by the create-urls DAG and 
- scrap_vocabulary : Extraction phase -> Contains functions to get data.
- process_vocabulary : Process the data to add the number of chars of a word, the similarity score between a word and its translation and its frequency of use score.
- predict_difficulty : Task to attribute a level of difficulty for each word according to the number of chars and the similarity. For example, 'neu' is easier to learn than 'geheimnisvoll' because there are only 3 characters and looks like 'nouveau' in French. To define the level, I used a no-supervised algorithm approch by training a K-means model with the file [data.csv](app\airflow\data\data.csv) and creating a pipeline for the next predictions.
**To be more accurate, the algorithm needs more data. For now, I chose to train the model with approximately 1000 words.**

## Features
### Get URLs of vocabularies list
For now, it is possible to get the URLs of vocabulary lists to scrap words. Those URLs are stored in MONGODB in order for the goelern-etl pipeline to select 
a data which has a WAITING status.
Example of parameters which is stored in MONGODB :

![parameter_scrap](/readme_images/parameter_scrap.png)

### Get words
For now, you can get words using the goelern-etl DAG. The data is for now saved on s3.
When a URL is treated, its status is changed, so it can't be scraped again: 

![parameter_scrap](/readme_images/treated.png)



 

## Future Enhancements
- [ ] Save words on MONGODB at the end of the DAG
- [ ] Verify and clean the existing code
- [ ] Rename difficulty_model.Pkl for each language
- [ ] Create an interface where a user can create queries for airflow pipeline by providing the url and language to scrap 
- [ ] Test with window functions and CTEs to practice. Those functions will be called using an API
- [ ] Exercise : Chose a category and random word in French or German will be provided and we have to find its translation.
If the user find the word correctly, the score of the word is reduced by one, else, it is incremented by one. The objective is to often propose the words which have the greatest score. To do so, we can use probabilities
- [ ] Write a complete README with functionnalities and how to execute the code
- [ ] Generate some sentences using a LLM to understand how to use a word 
- [ ] Add Unit test
- [ ] Use airflow to call periodically the tasks : get data and send every hour a word with its translation. If the score of a word is high, this one has a lot of chance to be chosen to be sent for the user
