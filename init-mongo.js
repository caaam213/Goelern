
db = db.getSiblingDB('goelerndb');
db.createCollection("words");
db.createCollection("constants");
db.constants.insertMany([
    {
        "language": "de",
        "base_name_col": "german",
        "trans_name_col": "french",
        "crawling_url": "https://fichesvocabulaire.com/vocabulaire-allemand-pdf"
    },
    {
        "language": "en",
        "base_name_col": "english",
        "trans_name_col": "french",
        "crawling_url": "https://fichesvocabulaire.com/listes-vocabulaire-anglais-pdf"
    }
]);