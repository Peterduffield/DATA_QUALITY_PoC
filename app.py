from snowflake.snowpark import Session
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import os

# Create a function to connect using Snowpark
SF_CREDENTIALS = {
    "account": "jsa18243",
    "user": "lkr_python_runner",
    "password": "pythonpassword",
    "role": "DATA_ENGINEER",
    "warehouse": "DEMO_WH",
    "database": "DATA_GOV_POC",
    "schema": "POC_TABLES"
}

def create_snowflake_session():
    return Session.builder.configs(SF_CREDENTIALS).create()
session = create_snowflake_session()

# Run SQL query
dq_meta_source_table = session.sql("SELECT * FROM DATA_GOV_POC.DATA_QUALITY_POC.DATA_QUALITY_RULES").to_pandas()


def main():
    st.set_page_config(layout="wide")
    st.markdown(
        """
        <style>
        .title-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            text-align: center;
        }
        .title-container h1 {
            font-size: 2.5em; /* Adjust size as needed */
            margin-bottom: 45px; /* Spacing between title and subtitle */
        }
        </style>
        <div class="title-container">
            <h1>Data Quality Overview</h1>
        </div>
        """,
        unsafe_allow_html=True
    )

    dq_by_table, dq_by_db, dq_by_data_soource =  st.tabs(['Data Quality by Table', 'Data Quality by DataBase', 'Data Quality by Data Source'])
    with dq_by_table:
        dq_meta_table = dq_meta_source_table
        selected_table = st.selectbox("Select a Table:", "SALESFORCE_DONORS_PATIENTS_DATASET")
        if selected_table:
            dq_meta_table = dq_meta_source_table[dq_meta_source_table["TABLE_NAME"] == selected_table]
            st.divider()
            col1, col2, col3 = st.columns(3, border = True)
            with col1:
                st.write(":page_facing_up: Columns Tested")
                columns_tested = dq_meta_table['COLUMN_TESTED'].nunique()
                st.markdown(
                    f"""
                    <div style='text-align: center;'>
                        <h2>{columns_tested}</h2>
                    </div>
                    """,
                    unsafe_allow_html=True)                
            with col2:
                st.write(":straight_ruler: Data Quality Rules")
                rules_tested = dq_meta_table['RULE_NAME'].nunique()
                st.markdown(
                    f"""
                    <div style='text-align: center;'>
                        <h2>{rules_tested}</h2>
                    </div>
                    """,
                    unsafe_allow_html=True)                
            with col3:
                st.write(":white_check_mark: Passed Data Quality Rules")


            col4,col5,col6 = st.columns(3, border = True)
            st.dataframe(dq_meta_table)

            
    with dq_by_db:
        st.markdown(
        """
        <style>
        .container {
            display: flex;
            justify-content: center;
        }
        .container img {
            transform: scale(10);
        }
        </style>
        <div class="container">
            <img src="https://pngimg.com/d/under_construction_PNG18.png" alt="Hakkoda Logo">

        </div>
        """,
        unsafe_allow_html=True,
        )
    with dq_by_data_soource:
        st.write("Under Construction")
        st.markdown(
        """
        <style>
        .container {
            display: flex;
            justify-content: center;
        }
        .container img {
            transform: scale(10);
        }
        </style>
        <div class="container">
            <img src="https://pngimg.com/d/under_construction_PNG18.png" alt="Hakkoda Logo">

        </div>
        """,
        unsafe_allow_html=True,
        )      

    st.markdown(
        """
        <style>
        .container {
            display: flex;
            justify-content: center;
        }
        .container img {
            transform: scale(0.2);
        }
        </style>
        <div class="container">
            <img src="https://tercera.io/wp-content/uploads/2025/04/Hakkoda_IBM.jpg" alt="Hakkoda Logo">

        </div>
        """,
        unsafe_allow_html=True,
    ) 


if __name__ == "__main__":
    main()