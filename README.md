# Application to learn German : Goelern

## Author 
MERAOUI Camelia

## Description
Initially, the goal is to create an app to learn German using data from https://fichesvocabulaire.com/ and AI.

I created this project for many reasons :
- Learn Pyspark 
- Practice my data engineering skills
- Learn SQL Window function
- Improve my skills in AI 
- Learn Airflow 

Now, it is possible to get vocabulary for every languages in the website.
For now, the graphical interface will only be for the German vocabulary

## Used technologies
- Python
- PySpark
- Streamlit
- SQLLite 
- MongoDB

## Tasks
### Vocabulary lists
- [x] Configure the Pipfile and the Docker files
- [x] Implement script to extract vocabulary lists 
- [x] Process data to add features for ML to find the difficulty of a word : 
    - Extract the number of letters for German words
    - Similarity score between french and english (using which library ?)
- [x] Find the frequency of a word generally used
- [x] Configure the DB
- [x] Save recuperated data on S3
- [x] Store data on sqllite  
- [x] Set the architecture of the project 
- [x] Display word list by category and sort by frequencies or alphabetical order
- [x] Verify URL format 
- [x] Transform data from ML (normalization, etc.)
- [x] Create etl classes in order to manage several languages
- [x] Save in csv format when scraping for the model
- [x] Calculate score using number_char and similarity score
- [x] Use Machine Learning to predict the difficulty of a word according to the number of letters and the similarity score. The goal is to attribute a starting score
- [ ] Verify if a same word in german is not added in db
- [ ] Test with window functions and CTEs to practice. Those functions will be called using an API
- [ ] Exercise : Chose a category and random word in French or German will be provided and we have to find its translation.
If the user find the word correctly, the score of the word is reduced by one, else, it is incremented by one. The objective is to often propose the words which have the greatest score. To do so, we can use probabilities
- [ ] Write a complete README with functionnalities and how to execute the code
- [ ] Generate some sentences using a LLM to understand how to use a word 
- [ ] Add Unit test
- [ ] Add CD/CI in order to manage modifications
- [ ] Use airflow to call periodically the tasks : get data and send every hour a word with its translation. If the score of a word is high, this one has a lot of chance to be chosen to be sent for the user

