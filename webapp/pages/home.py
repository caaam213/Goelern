import streamlit as st

from webapp.pages.vocabulary_list import display_vocab_list


def main():

    page = st.sidebar.selectbox("Select a page", ["Home", "Vocabulary List"])

    if page == "Home":
        st.title("Welcome to the Vocabulary App")
        st.write("Welcome to the Vocabulary app! Use the sidebar to navigate.")
    elif page == "Vocabulary List":
        display_vocab_list()


if __name__ == "__main__":
    main()
