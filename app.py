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

from datetime import datetime

from datetime import datetime

def evaluate_rules(dq_meta_table: pd.DataFrame, session: Session) -> pd.DataFrame:
    # Get the current timestamp to use for "LAST_RUN"
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for idx, row in dq_meta_table.iterrows():
        rule_sql = row["RULE_SQL"]
        result = None
        status = "UNKNOWN"

        try:
            result_df = session.sql(rule_sql).to_pandas()

            if not result_df.empty:
                result = result_df.iloc[0, 0]

                # Evaluate against ACCEPTED_THRESHOLD_PCT column
                if "ACCEPTED_THRESHOLD_PCT" in row and pd.notna(row["ACCEPTED_THRESHOLD_PCT"]):
                    threshold = float(row["ACCEPTED_THRESHOLD_PCT"])
                    status = "PASS" if float(result) <= threshold else "FAIL"
                else:
                    status = "PASS"  # Default if no threshold
            else:
                result = "No result"
                status = "FAIL"

        except Exception as e:
            result = f"Error: {str(e)}"
            status = "ERROR"

        # Update the 'RESULT', 'STATUS', and 'LAST_RUN' columns
        dq_meta_table.at[idx, "RESULT"] = result
        dq_meta_table.at[idx, "STATUS"] = status
        dq_meta_table.at[idx, "LAST_RUN"] = current_time

    return dq_meta_table
def safe_str(value):
    """Escape single quotes in strings to prevent SQL errors."""
    return value.replace("'", "''") if isinstance(value, str) else value



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
                st.write(":straight_ruler: Data Quality Tests")
                rules_tested = dq_meta_table['RULE_NAME'].nunique()
                st.markdown(
                    f"""
                    <div style='text-align: center;'>
                        <h2>{rules_tested}</h2>
                    </div>
                    """,
                    unsafe_allow_html=True)                
            with col3:
                st.write(":clock1: Last Updated")
                last_updated = dq_meta_table['LAST_RUN'].max()
                last_updated_str = last_updated.strftime('%Y-%m-%d')
                st.markdown(
                    f"""
                    <div style='text-align: center;'>
                        <h2>{last_updated_str}</h2>
                    </div>
                    """,
                    unsafe_allow_html=True)                 
            col4,col5,col6 = st.columns(3, border = True)
            with col4:  
                st.write("Test Status by Rule Category")
                passed_tests_counts = dq_meta_table.groupby(["RULE_CATEGORY","STATUS"]).size().unstack(fill_value=0)
                st.bar_chart(passed_tests_counts,use_container_width=True, color=["#5dade2", "#a9cce3"],horizontal=True)

                st.write("Test Status by Column")
                passed_tests_counts = dq_meta_table.groupby(["COLUMN_TESTED","STATUS"]).size().unstack(fill_value=0)
                st.bar_chart(passed_tests_counts,use_container_width=True, color=["#7fd787", "#f06f6f"],horizontal=True)

            with st.popover("View Data Quality Tests Table", use_container_width=True):
                st.dataframe(dq_meta_table, hide_index=True)

            if st.button("Run Data Quality Checks", use_container_width=True):
                # Call the function to evaluate rules and get the results
                dq_result_table = evaluate_rules(dq_meta_table.copy(), session)
                
                # Optionally: Update the table in Snowflake (you can add this step in the same block or after)
                try:
                    for idx, row in dq_result_table.iterrows():
                        update_query = f"""
                            MERGE INTO DATA_GOV_POC.DATA_QUALITY_POC.DATA_QUALITY_RULES AS target
                            USING (SELECT {row['RULE_ID']} AS RULE_ID) AS source
                            ON target.RULE_ID = source.RULE_ID
                            WHEN MATCHED THEN
                                UPDATE SET 
                                    RESULT = '{safe_str(row['RESULT'])}',
                                    STATUS = '{safe_str(row['STATUS'])}',
                                    LAST_RUN = '{safe_str(row['LAST_RUN'])}'
                            WHEN NOT MATCHED THEN
                                INSERT (RULE_ID, RESULT, STATUS, LAST_RUN)
                                VALUES ({row['RULE_ID']}, '{safe_str(row['RESULT'])}', '{safe_str(row['STATUS'])}', '{safe_str(row['LAST_RUN'])}');
                        """
                        session.sql(update_query).collect()  # Executing the query in Snowflake
                    st.success("Snowflake table updated successfully!")
                except Exception as e:
                    st.error(f"Error updating Snowflake table: {e}")


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