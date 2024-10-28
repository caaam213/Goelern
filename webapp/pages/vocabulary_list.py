import streamlit as st
from app.services.de_vocab_service import DeVocabService
from webapp.constants.constants import ALL, LIST_OF_ORDER_TYPES, LIST_OF_SORT_FIELDS


def display_vocab_list():
    st.title("Vocabulary List")

    # # Get the data from the database
    # vocab_service = DeVocabService()
    # categories = vocab_service.get_all_vocabs("German_vocabulary")

    # sort_field = st.selectbox("Sort the list by :", LIST_OF_SORT_FIELDS)

    # sort_order = st.radio("Sort order :", LIST_OF_ORDER_TYPES)

    # # Display category list
    # categories_list = [category for category in categories]
    # categories_list.insert(0, ALL)
    # category_field = st.selectbox("Categories :", categories_list)

    # if st.button("Validate"):
    #     df_vocab = vocab_service.sort(category_field, sort_field, sort_order)

    #     # Display data in table format
    #     st.dataframe(df_vocab)
