# nohup streamlit run dashboard.py --server.address=0.0.0.0 --server.port=8501 &

import pandas as pd
import re
from wordcloud import STOPWORDS
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import streamlit as st

data = pd.read_csv("ecommerce_products_20250531_194248.csv")

categories = data[data["brand"].notna()]["brand"].unique()
# 26 Unique Brands

brand_summary = data.groupby('brand').agg(
    Basket_Cost=('price_numeric', 'sum'),
    Item_Count=('sku', 'count')
).reset_index()

# This helps answer: Is a brand expensive because it has many items, or because each item is costly?
# Add average item price
brand_summary['Avg_Item_Price'] = brand_summary['Basket_Cost'] / brand_summary['Item_Count']

# Plotly bubble chart
fig = px.scatter(
    brand_summary,
    x='Item_Count',
    y='Basket_Cost',
    size='Avg_Item_Price',
    color='brand',
    hover_name='brand',
    # text='brand',
    size_max=60,
    title='Brand Basket Cost vs Item Count (Bubble = Avg Price)'
)

fig.update_traces(textposition='top center')
fig.update_layout(
    xaxis_title='Number of Items',
    yaxis_title='Total Basket Cost',
    legend_title='Brand'
)
st.plotly_chart(fig, use_container_width=True)