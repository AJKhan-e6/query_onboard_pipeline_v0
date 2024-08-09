import pandas as pd
import requests
import subprocess
import os
import signal
import streamlit as st
import time
import base64
# import sqlglot
import sqlparse
# from sqlglot import exp, parse_one

def start_java_parser(schema_content):
    env = os.environ.copy()
    env["DO_NOT_EXECUTE"] = "true"

    # Write the uploaded schema content to a temporary file
    schema_txt_path = "/tmp/schema.txt"
    with open(schema_txt_path, 'w') as f:
        f.write(schema_content)

    java_command = [
        "java",
        "-jar",
        "/Users/abduljawadkhan/Downloads/ml-projects/transferable/e6-engine-SNAPSHOT-jar-with-dependencies.jar",
        schema_txt_path,
    ]
    process = subprocess.Popen(java_command, env=env)
    return process

def stop_java_parser(process):
    try:
        process.terminate()
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.kill(process.pid, signal.SIGKILL)

def send_to_parser_api(catalog_name, db_name, sql_query):
    sql_query = sql_query.rstrip(";")
    url = f"http://localhost:10001/parse-plan-query?catalog={catalog_name}&schema={db_name}"
    response = requests.post(url, data=sql_query)
    return response

def convert_query(query, from_sql, to_sql):
    converted_query = None
    if query:
        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=True)[0]
    return converted_query

def process_queries(input_csv, schema_content, catalog_name, db_name, from_sql, to_sql):
    df = pd.read_csv(input_csv)
    df['Parsing Passed First Try'] = 'NO'
    df['Parsing Failed First Try'] = 'NO'
    df['Error Message'] = ''
    df['Transpiling Passed'] = 'NO'
    df['Transpiling Failed'] = 'NO'
    df['Transpiled Query'] = ''
    df['Transpiling Passed Parsing Passed'] = 'NO'
    df['Transpiling Passed Parsing Failed'] = 'NO'
    df['Transpiling Passed Parsing Failed Error'] = ''

    for index, row in df.iterrows():
        sql_query = row['QUERY_TEXT']
        response = send_to_parser_api(catalog_name, db_name, sql_query)
        
        if response.status_code == 200 and response.text.strip() == "SUCCESS":
            df.at[index, 'Parsing Passed First Try'] = 'YES'
        else:
            df.at[index, 'Parsing Failed First Try'] = 'YES'
            df.at[index, 'Error Message'] = response.text.strip()

            # Convert the failed query
            converted_query = convert_query(sql_query, from_sql, to_sql)
            if converted_query:
                df.at[index, 'Transpiling Passed'] = 'YES'
                df.at[index, 'Transpiled Query'] = converted_query
                
                # Send the transpiled query back to the parser
                transpiled_response = send_to_parser_api(catalog_name, db_name, converted_query)
                
                if transpiled_response.status_code == 200 and transpiled_response.text.strip() == "SUCCESS":
                    df.at[index, 'Transpiling Passed Parsing Passed'] = 'YES'
                else:
                    df.at[index, 'Transpiling Passed Parsing Failed'] = 'YES'
                    df.at[index, 'Transpiling Passed Parsing Failed Error'] = transpiled_response.text.strip()
            else:
                df.at[index, 'Transpiling Failed'] = 'YES'

    return df

# Streamlit code
st.set_page_config(page_title="Query Parser and Converter", layout="centered", initial_sidebar_state="auto")
st.title("Query Parser and Converter")

st.info("Note: The CSV file must contain columns named 'QUERY_TEXT' and 'UNQ_ALIAS'.")
uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])
schema_file = st.file_uploader("Upload Schema Text File", type=["txt"])

from_sql = st.selectbox("From SQL", ["snowflake", "databricks", "athena", "presto", "postgres", "bigquery", "E6", "trino"],
                        key="csv_from_sql")
to_sql = st.selectbox("To SQL", ["snowflake", "databricks", "athena", "presto", "postgres", "bigquery", "E6", "trino"],
                      key="csv_to_sql")

if uploaded_file is not None and schema_file is not None:
    if st.button("Process CSV"):
        start_time = time.time()
        
        # Read the schema content
        schema_content = schema_file.read().decode('utf-8')

        catalog_name = 'your_catalog_name'
        db_name = 'your_db_name'

        parser_process = start_java_parser(schema_content)
        
        try:
            df = process_queries(uploaded_file, schema_content, catalog_name, db_name, from_sql, to_sql)
        finally:
            stop_java_parser(parser_process)

        response_csv = df.to_csv(index=False)
        b64 = base64.b64encode(response_csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="processed_results.csv">Download Processed Results CSV</a>'
        st.markdown(href, unsafe_allow_html=True)

        total_time = time.time() - start_time
        st.write(f"Total time taken for this whole CSV to generate is {total_time:.2f}s")
