from app.etl.extraction.crawling_voc_list_urls import CrawlingVocListUrls
from app.etl.extraction.scrap_vocabulary import ScrapVocabulary
from app.etl.store.store_vocabulary import StoreVocabulary
from app.etl.transformation.ml.predict_difficulty import PredictDifficulty
from app.etl.transformation.process_vocabulary import ProcessVocabulary


if __name__ == "__main__":
    lang = "en"
    # Crawling (Extraction phase)
    crawling_obj = CrawlingVocListUrls(lang)
    crawling_data = crawling_obj.run()

    # Scrap (Extraction phase)
    scrap_url = crawling_data[0].get("scrap_url", "")
    scraping_obj = ScrapVocabulary()
    scraped_data = scraping_obj.run(scrap_url=scrap_url)

    # Process data (Transformation phase)
    process_obj = ProcessVocabulary(crawling_obj.data_lang)
    processed_data = process_obj.run(scraped_data)

    # Calculate the difficulty of each word (Transformation phase)
    ml_difficulty = PredictDifficulty(crawling_obj.data_lang)
    predictions = ml_difficulty.run(processed_data)
    processed_data["difficulty"] = predictions
    print(processed_data)

    # Store the data (Store phase)
    StoreVocabulary(lang).run(processed_data)
