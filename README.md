# Application to learn German : Goelern

## Description
The goal of this project is to get German words and their French translations from https://fichesvocabulaire.com and to add 

## Used technologies


1. Process data to add features for ML to find the difficulty of a word : 
    - Extract the number of letters for German words
    - Similarity score between french and english (using which library ?)
2. Store data on sqllite ? 
3. Use Machine Learning to predict the difficulty of a word according to the number of letters and the similarity score. The goal is to attribute a starting score
4. Use airflow to call perodically the tasks : get data and send every hour a word with its translation. If the score of a word is high, this one has a lot of chance to be chosen to be sent for the user
5. Exercise : Chose a category and random word in French or German will be provided and we have to find its translation.
If the user find the word correctly, the score of the word is reduced by one, else, it is incrementated by one. The objective is to often propose the words which have the greatest score. To do so, we can use probabilities