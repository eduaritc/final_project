import pandas as pd
import plotly.express as px
import streamlit as st
import os

FOLDER = "./amazon_reviews/"



def sales_by_size(df_filtered):
    """_summary_
    It returns a chart with the sales grouped by size
    Args:
        df_filtered (DataFrame): Pandas Data Frame with the information
    
    Returns:
        BarChart: chart with sales grouped by size
    """
    size_sales = df_filtered.groupby(by=["size"]).sum()[["price"]]
    fig_size_sales = px.bar(
        size_sales,
        x = size_sales.index,
        y = "price",
        title =  "<b> Sales by size </b>",
        color_discrete_sequence = ["#0083B8"] * len(size_sales),
        template = "plotly_white"
    )

    fig_size_sales.update_layout(
        xaxis = dict(tickmode = "linear"),
        plot_bgcolor = "rgba(0,0,0,0)",
        yaxis = dict(showgrid = False)

    )
    return(fig_size_sales)


def sales_by_colour(df_filtered):
    """_summary_
    It returns a chart with the sales grouped by colour
    Args:
        df_filtered (DataFrame): Pandas Data Frame with the information
    
    Returns:
        BarChart: chart with sales grouped by colour
    """
    colour_sales = df_filtered.groupby(by=["colour"]).sum()[["price"]]
    fig_colour_sales = px.bar(
        colour_sales,
        x = colour_sales.index,
        y = "price",
        title =  "<b> Sales by Colour </b>",
        color_discrete_sequence = ["#0083B8"] * len(colour_sales),
        template = "plotly_white"
    )

    fig_colour_sales.update_layout(
        xaxis = dict(tickmode= "linear"),
        plot_bgcolor = "rgba(0,0,0,0)",
        yaxis = (dict(showgrid = False))
    )
    return(fig_colour_sales)



def sales_by_country(df_filtered):
    """_summary_
    It returns a chart with the sales grouped by country
    Args:
        df_filtered (DataFrame): Pandas Data Frame with the information
    
    Returns:
        BarChart: chart with sales grouped by country
    """
    country_sales = (
        df_filtered.groupby(by=["country"]).sum()[["price"]].sort_values(by="price")
    )
    fig_country_sales = px.bar(
        country_sales,
        x = "price",
        y = country_sales.index,
        orientation = "h",
        title =  "<b> Sales by Country </b>",
        color_discrete_sequence = ["#0083B8"] * len(country_sales),
        template = "plotly_white"
    )

    fig_country_sales.update_layout(
        plot_bgcolor = "rgba(0,0,0,0)",
        xaxis = dict(showgrid = False)

    )
    return(fig_country_sales)



def create_mainpage(st_dashboard, df_filtered):
    """_summary_
    Dashboard's mainpage with the avg sales per item
    Args:
        st_dashboard (Streamlit): Streamlit object to handle the dashboard
        df_filtered (Data Frame): Pandas Dataframe with the filtered data.
    """
    num_sales = int(df_filtered["colour"].count())
    avg_stars = round((df_filtered["stars"]).mean(), 1)
    star_rating = ":star:" * int(str(round(avg_stars)), 0)
    avg_sales = round(df_filtered["price"].mean(), 2)

    left_column, middle_column, right_column = st_dashboard.columns(3)

    with left_column:
        st_dashboard.subheader("Number of sales")
        st_dashboard.subheader(num_sales)
    with middle_column:
        st_dashboard.subheader("Average rating")
        st_dashboard.subheader("{} out of 5".format(avg_stars))
    with right_column:
        st_dashboard.subheader("Avg Sales per item")
        st_dashboard.subheader("{} Pounds".format(avg_sales))
    st_dashboard.markdown("===")



def create_sidebar(st_dashboard, df_data):
    """_summary_
    It Creates a sidebar with filters
    Args:
        st_dashboard (streamlit): It's the object that contains the dashboard and its options
        df_data (DataFrame): Pandas DataFrame with the data

    Returns:
        DataFrame: Pandas Data Frame with the data filtered
    """
    st_dashboard.sidebar.header("Please Filter Here:")

    colour = st.sidebar.multiselect(
        "select the colour:",
        options = df_data["colour"].unique(),
        default = df_data["colour"].unique()
    )

    country = st_dashboard.sidebar.multiselect(
        "select the country:",
        options = df_data["country"].unique(),
        default = df_data["country"].unique()
        )
    
    date = st_dashboard.sidebar.multiselect(
        "select the date:",
        options = df_data["date"].unique(),
        default = df_data["date"].unique()
        )
    
    size = st_dashboard.sidebar.multiselect(
        "select the size:",
        options = df_data["size"].unique(),
        default = df_data["size"].unique()
        )
    
    stars = st_dashboard.sidebar.multiselect(
        "select the number of stars:",
        options = df_data["stars"].unique(),
        default = df_data["stars"].unique()
        )
    df_selection = df_data.query(
        "colour == @colour & country == @country & date == @date & size == @size & stars == @stars"
    )
    return df_selection


def get_filename(folder):
    """_summary_
    Args:
        folder (String): Foldername where the CSV file is being storage

    Returns:
        String: name of the csv file with the data to load on the dashboard
    """
    filename = None
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file[-3:] == "csv":
                filename = file
                break
                break
    return filename


filename = get_filename(os.path.abspath("./amazon_reviews"))
# print(filename)
st.set_page_config(page_title="Sales Dashboard", page_icon=":pound:", layout="wide")
df_reviews = pd.read_csv(FOLDER+filename, on_bad_lines='skip')
# st.dataframe(df_sales)
df_filters = create_sidebar(st, df_reviews) 
# st.dataframe(df_filters)
create_mainpage(st, df_filters)
sales_country = sales_by_country(df_filters)
sales_colour = sales_by_colour(df_filters)
sales_size = sales_by_size(df_filters)

left_column, middle_column, right_column = st.columns(3)
left_column.plotly_chart(sales_country, use_container_width= True)
middle_column.plotly_chart(sales_size, use_container_width= True)
right_column.plotly_chart(sales_colour, use_container_width= True)

hide_st_stle = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            """
st.markdown(hide_st_stle, unsafe_allow_html= True)
