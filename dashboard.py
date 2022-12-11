import pandas as pd
import plotly.express as px
import streamlit as st
import os

FOLDER = "./amazon_reviews/"



def get_filename(folder):
    filename = None
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file[-3:] == "csv":
                filename = file
                break
                break
    return filename


# os.path.abspath("./amazon_reviews")
# df_pd_csv = hive_transfer.df_csv.toPandas()
filename = get_filename(os.path.abspath("./amazon_reviews"))
st.set_page_config(page_title="Sales Dashboard", page_icon=":pound:", layout="wide")
st.dataframe(pd.read_csv(FOLDER+filename))





