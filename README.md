# Application to learn German : Goelern

## Author 
MERAOUI Camelia

## Description
The goal of this project is to improve the learning of German (for now) using data from https://fichesvocabulaire.com/ and AI.

I created this project for many reasons :
- Learn Pyspark and Airflow 
- Practice my data engineering skills
- Learn SQL Window function
- Improve my skills in AI 
- Obviously, to use it for my German Learning Journey ! 

Next, the goal is to get German articles, grammatical rules and podcasts to exploit them with AI to improve the learning of this incredible language ! 

## Used technologies
- Python
- PySpark
- Airflow
- Streamlit
- FastAPI
- SQLLite 

## Tasks
### Vocabulary lists
- [x] Configure the Pipfile and the Docker files
- [x] Implement script to extract vocabulary lists 
- [x] Process data to add features for ML to find the difficulty of a word : 
    - Extract the number of letters for German words
    - Similarity score between french and english (using which library ?)
- [x] Find the frequency of a word generally used **(WordFreq library could disappear so maybe find another solution ?)**
- [x] Configure the DB
- [ ] Save recuperated data on S3
- [ ] Store data on sqllite  
- [ ] Use airflow to call periodically the tasks : get data and send every hour a word with its translation. If the score of a word is high, this one has a lot of chance to be chosen to be sent for the user
- [ ] Set the architecture of the project 
- [ ] Configure API structure
- [ ] Test with window functions and CTEs to practice. Those functions will be called using an API
- [ ] Transform data from ML (normalization, etc.)
- [ ] API endpoint to generate a .txt with words in format "word:translation" (with annotation ?) for anki, quizlet and so on 
- [ ] Calculate score using number_char, frequency, similarity
- [ ] API endpoint to generate a file which is a summary of the learning
- [ ] When getting the verbs, verify if it is irregular ? (Maybe when getting the list from another website)
- [ ] Use Machine Learning to predict the difficulty of a word according to the number of letters and the similarity score. The goal is to attribute a starting score
- [ ] Exercise : Chose a category and random word in French or German will be provided and we have to find its translation.
If the user find the word correctly, the score of the word is reduced by one, else, it is incremented by one. The objective is to often propose the words which have the greatest score. To do so, we can use probabilities
- [ ] Generate some sentences using a LLM to understand how to use a word 
- [ ] Add Unit test
- [ ] Add CD/CI in order to manage modifications