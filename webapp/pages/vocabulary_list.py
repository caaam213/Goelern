import streamlit as st
from app.services.vocab_service import VocabService
from constants.constants import ALL, LIST_OF_ORDER_TYPES, LIST_OF_SORT_FIELDS

def display_vocab_list():
    st.title("German Vocabulary List")

    # Get the data from the database
    vocab_service = VocabService() 
    data = vocab_service.get_all_vocabs()
    categories = vocab_service.get_category_list()

    sort_field = st.selectbox(
        "Sort the list by :",
        LIST_OF_SORT_FIELDS 
    )

    sort_order = st.radio(
        "Sort order :",
        LIST_OF_ORDER_TYPES
    )

    # Display category list
    categories_list = [category for category in categories]
    categories_list.insert(0, ALL)
    category_field = st.selectbox(
        "Categories :",
        categories_list
    )

    if st.button("Validate"):
        df_vocab = vocab_service.sort(category_field, sort_field, sort_order)
        
        # Display data in table format
        st.dataframe(df_vocab)
