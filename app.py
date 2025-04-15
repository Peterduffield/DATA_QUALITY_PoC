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
business_glossary_tbl = session.sql("SELECT * FROM BUSINESS_GLOSSARY").to_pandas()
data_catalog_tbl = session.sql("SELECT * FROM DATA_CATALOG").to_pandas()
employee_tbl = session.sql("SELECT * FROM EMPLOYEE_CATALOG order by EMPLOYEE_ID asc").to_pandas()
employee_use_case_catalog_tbl = session.sql("SELECT e.*, i.* FROM EMPLOYEE_CATALOG e LEFT JOIN USE_CASE_INVENTORY_TBL i ON e.EMPLOYEE_NAME = i.BUSINESS_STAKEHOLDER order by e.EMPLOYEE_ID asc").to_pandas()
employee_glossary_tbl = session.sql("SELECT DISTINCT e.*,  bg.* from EMPLOYEE_CATALOG e LEFT JOIN BUSINESS_GLOSSARY bg ON bg.Data_Owner_Employee_Name = e.EMPLOYEE_NAME OR bg.Data_Steward_Employee_Name = e.EMPLOYEE_NAME WHERE e.GOVERNANCE_ROLE = 'Data Owner' or e.GOVERNANCE_ROLE = 'Data Steward'").to_pandas()
employee_catalog_tbl = session.sql("SELECT DISTINCT e.*,  dc.* from EMPLOYEE_CATALOG e LEFT JOIN DATA_CATALOG dc ON dc.Data_Custodian = e.EMPLOYEE_NAME OR dc.Technical_Data_Steward = e.EMPLOYEE_NAME WHERE e.GOVERNANCE_ROLE = 'Data Custodian' or e.GOVERNANCE_ROLE = 'Technical Data Steward'").to_pandas()
use_case_inventory_tbl = session.sql("SELECT * from USE_CASE_INVENTORY_TBL").to_pandas()

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
    if dq_by_table:
        selected_table = st.selectbox("Select a Table:", "SALESFORCE_DONORS_PATIENTS_DATASET")
        st.divider()
    if dq_by_db:
        st.write("Under Construction")
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
            <img src="https://t4.ftcdn.net/jpg/00/89/02/67/360_F_89026793_eyw5a7WCQE0y1RHsizu41uhj7YStgvAA.jpg" alt="Hakkoda Logo">

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