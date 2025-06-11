# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime
import re
import base64
import io

# Set page config
st.set_page_config(
    page_title="Snowflake Validation Automation Tool",
    page_icon="❄️",
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
<style>
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        border-radius: 5px;
        padding: 0.5rem 1rem;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
    .stSelectbox, .stTextInput {
        margin-bottom: 1rem;
    }
    .stDataFrame {
        width: 100%;
    }
    .stAlert {
        padding: 1rem;
        border-radius: 5px;
    }
    .logo {
        text-align: center;
        margin-bottom: 1rem;
    }
    .tab-content {
        padding: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ========== SNOWFLAKE FUNCTIONS ==========
def get_snowflake_connection(user, password, account):
    """Establish connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            authenticator='snowflake'
        )
        return conn, "✅ Successfully connected!"
    except Exception as e:
        return None, f"❌ Connection failed: {str(e)}"

def disconnect_snowflake(conn):
    """Close Snowflake connection"""
    if conn:
        conn.close()
    return None, "🔌 Disconnected successfully"

def get_databases(conn):
    """Get list of databases"""
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        st.error(f"Error getting databases: {str(e)}")
        return []

def get_schemas(conn, database):
    """Get schemas for specific database"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        st.error(f"Error getting schemas: {str(e)}")
        return []

def clone_schema(conn, source_db, source_schema, target_schema):
    """Clone schema with improved error handling and reporting"""
    cursor = conn.cursor()
    try:
        # First check if source schema exists
        cursor.execute(f"SHOW SCHEMAS LIKE '{source_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Source schema {source_db}.{source_schema} doesn't exist", pd.DataFrame()

        # Execute clone command
        cursor.execute(
            f"CREATE OR REPLACE SCHEMA {source_db}.{target_schema} "
            f"CLONE {source_db}.{source_schema}"
        )

        # Verify clone was successful
        cursor.execute(f"SHOW SCHEMAS LIKE '{target_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Clone failed - target schema not created", pd.DataFrame()

        # Get list of cloned tables
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{source_schema}")
        source_tables = [row[1] for row in cursor.fetchall()]

        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{target_schema}")
        clone_tables = [row[1] for row in cursor.fetchall()]

        # Create summary DataFrame
        df_tables = pd.DataFrame({
            'Database': source_db,
            'Source Schema': source_schema,
            'Clone Schema': target_schema,
            'Source Tables': len(source_tables),
            'Cloned Tables': len(clone_tables),
            'Status': '✅ Success' if len(source_tables) == len(clone_tables) else '⚠️ Partial Success'
        }, index=[0])

        return True, f"✅ Successfully cloned {source_db}.{source_schema} to {source_db}.{target_schema}", df_tables
    except Exception as e:
        return False, f"❌ Clone failed: {str(e)}", pd.DataFrame()

def compare_table_differences(conn, db_name, source_schema, clone_schema):
    """Compare tables between schemas"""
    cursor = conn.cursor()

    query = f"""
    WITH source_tables AS (
        SELECT table_name
        FROM {db_name}.information_schema.tables
        WHERE table_schema = '{source_schema}'
    ),
    clone_tables AS (
        SELECT table_name
        FROM {db_name}.information_schema.tables
        WHERE table_schema = '{clone_schema}'
    )
    SELECT
        COALESCE(s.table_name, c.table_name) AS table_name,
        CASE
            WHEN s.table_name IS NULL THEN 'Missing in source - Table Dropped'
            WHEN c.table_name IS NULL THEN 'Missing in clone - Table Added'
            ELSE 'Present in both'
        END AS difference
    FROM source_tables s
    FULL OUTER JOIN clone_tables c ON s.table_name = c.table_name
    WHERE s.table_name IS NULL OR c.table_name IS NULL
    ORDER BY difference, table_name;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return pd.DataFrame(results, columns=['Table', 'Difference'])

def compare_column_differences(conn, db_name, source_schema, clone_schema):
    """Compare columns and data types between schemas"""
    cursor = conn.cursor()

    # Get common tables
    common_tables_query = f"""
    SELECT s.table_name
    FROM {db_name}.information_schema.tables s
    JOIN {db_name}.information_schema.tables c
        ON s.table_name = c.table_name
    WHERE s.table_schema = '{source_schema}'
    AND c.table_schema = '{clone_schema}';
    """

    cursor.execute(common_tables_query)
    common_tables = [row[0] for row in cursor.fetchall()]

    column_diff_data = []
    datatype_diff_data = []

    for table in common_tables:
        # Get source table description
        cursor.execute(f"DESCRIBE TABLE {db_name}.{source_schema}.{table}")
        source_desc = cursor.fetchall()
        source_cols = {row[0]: row[1] for row in source_desc}

        # Get clone table description
        cursor.execute(f"DESCRIBE TABLE {db_name}.{clone_schema}.{table}")
        clone_desc = cursor.fetchall()
        clone_cols = {row[0]: row[1] for row in clone_desc}

        # Get all unique column names
        all_columns = set(source_cols.keys()).union(set(clone_cols.keys()))

        for col in all_columns:
            source_exists = col in source_cols
            clone_exists = col in clone_cols

            if not source_exists:
                column_diff_data.append({
                    'Table': table,
                    'Column': col,
                    'Difference': 'Missing in source - Column Dropped',
                    'Source Data Type': None,
                    'Clone Data Type': clone_cols.get(col)
                })
            elif not clone_exists:
                column_diff_data.append({
                    'Table': table,
                    'Column': col,
                    'Difference': 'Missing in clone - Column Added',
                    'Source Data Type': source_cols.get(col),
                    'Clone Data Type': None
                })
            else:
                # Column exists in both - check data type
                if source_cols[col] != clone_cols[col]:
                    datatype_diff_data.append({
                        'Table': table,
                        'Column': col,
                        'Source Data Type': source_cols[col],
                        'Clone Data Type': clone_cols[col],
                        'Difference': 'Data Type Changed'
                    })

    # Create DataFrames
    column_diff_df = pd.DataFrame(column_diff_data)
    if not column_diff_df.empty:
        column_diff_df = column_diff_df[['Table', 'Column', 'Difference', 'Source Data Type', 'Clone Data Type']]

    datatype_diff_df = pd.DataFrame(datatype_diff_data)
    if not datatype_diff_df.empty:
        datatype_diff_df = datatype_diff_df[['Table', 'Column', 'Source Data Type', 'Clone Data Type', 'Difference']]

    return column_diff_df, datatype_diff_df

def validate_kpis(conn, database, source_schema, target_schema, selected_kpis):
    """Validate KPIs between source and clone schemas"""
    cursor = conn.cursor()
    results = []

    # KPI names mapping to IDs (adjust based on your actual KPI table)
    kpi_mapping = {
        "Total Orders": 1,
        "Total Revenue": 2,
        "Average Order Value": 3,
        "Max Order Value": 4,
        "Min Order Value": 5,
        "Completed Orders": 6,
        "Cancelled Orders": 7,
        "Orders in April 2025": 8,
        "Unique Customers": 9
    }

    if not selected_kpis:
        return pd.DataFrame(), "⚠️ No KPIs selected for validation"

    try:
        # Fetch selected KPI definitions
        kpi_query = f"""
        SELECT KPI_ID, KPI_NAME, KPI_VALUE
        FROM {database}.{source_schema}.ORDER_KPIS
        WHERE KPI_NAME IN ({','.join([f"'{kpi}'" for kpi in selected_kpis])})
        """
        cursor.execute(kpi_query)
        kpis = cursor.fetchall()

        if not kpis:
            return pd.DataFrame(), "⚠️ No matching KPIs found in ORDER_KPIS table"

        # First verify both schemas have the ORDER_DATA table
        try:
            cursor.execute(f"SELECT 1 FROM {database}.{source_schema}.ORDER_DATA LIMIT 1")
            source_has_table = True
        except:
            source_has_table = False

        try:
            cursor.execute(f"SELECT 1 FROM {database}.{target_schema}.ORDER_DATA LIMIT 1")
            target_has_table = True
        except:
            target_has_table = False

        if not source_has_table or not target_has_table:
            error_msg = "ORDER_DATA table missing in "
            if not source_has_table and not target_has_table:
                error_msg += "both schemas"
            elif not source_has_table:
                error_msg += "source schema"
            else:
                error_msg += "target schema"

            for kpi_id, kpi_name, kpi_sql in kpis:
                results.append({
                    'KPI ID': kpi_id,
                    'KPI Name': kpi_name,
                    'Query': kpi_sql,
                    'Source Value': f"ERROR: {error_msg}",
                    'Clone Value': f"ERROR: {error_msg}",
                    'Difference': "N/A",
                    'Diff %': "N/A",
                    'Status': "❌ Error"
                })
            return pd.DataFrame(results), "❌ Validation failed - missing ORDER_DATA table"

        for kpi_id, kpi_name, kpi_sql in kpis:
            try:
                # More robust replacement that handles word boundaries and case
                source_query = re.sub(r'\bORDER_DATA\b', f'{database}.{source_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                cursor.execute(source_query)
                result_source = cursor.fetchone()[0] if cursor.rowcount > 0 else None
            except Exception as e:
                result_source = f"QUERY_ERROR: {str(e)}"

            try:
                clone_query = re.sub(r'\bORDER_DATA\b', f'{database}.{target_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                cursor.execute(clone_query)
                result_clone = cursor.fetchone()[0] if cursor.rowcount > 0 else None
            except Exception as e:
                result_clone = f"QUERY_ERROR: {str(e)}"

            # Calculate differences if possible
            diff = "N/A"
            pct_diff = "N/A"
            status = "⚠️ Mismatch"

            try:
                if (isinstance(result_source, (int, float)) and isinstance(result_clone, (int, float))):
                    diff = float(result_source) - float(result_clone)
                    pct_diff = (diff / float(result_source)) * 100 if float(result_source) != 0 else float('inf')
                    status = '✅ Match' if diff == 0 else '⚠️ Mismatch'
                elif str(result_source) == str(result_clone):
                    status = '✅ Match'
            except:
                pass

            results.append({
                'KPI ID': kpi_id,
                'KPI Name': kpi_name,
                'Query': kpi_sql,
                'Source Value': result_source,
                'Clone Value': result_clone,
                'Difference': diff if not isinstance(diff, float) else round(diff, 2),
                'Diff %': f"{round(pct_diff, 2)}%" if isinstance(pct_diff, float) else pct_diff,
                'Status': status
            })

        df = pd.DataFrame(results)
        return df, "✅ KPI validation completed"

    except Exception as e:
        return pd.DataFrame(), f"❌ KPI validation failed: {str(e)}"

# Helper function for download buttons
def get_table_download_link(df, filename):
    """Generates a link allowing the data in a given panda dataframe to be downloaded"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV File</a>'
    return href

# ========== STREAMLIT UI ==========
# Add logo and title
def main():
    # Set page config
    st.set_page_config(
        page_title="Snowflake Validation Automation Tool",
        page_icon="❄️",
        layout="wide"
    )

    st.image("data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAcYAAABCCAMAAAD66oZhAAAC91BMVEUAAAD///////////////////////////////////////////////////////////////////////////////////////////////////////////9C3P8fndX///////////////////////////////////////////////////////////////////////////////////////////////8fisgwxfr///////////9C3P////8adaL///////81u/n///////85zv1D3P86xv3///////////////////////8dhbg6zv7///850P8+0P////////8adaM2vfv///8/1v8egbH///8ztfY3v/s/0vv///9C2/v///8wqutB2v4adaI+0f4dfKowre8pns47yv3///////////////8vqutC2vsdfq8xsO7///8iib8/0/85xPk6yPIztfX///84xfkZdKUztvcadKJC3PNB2P8iib8+0f4ceKZB2f9C2/9C3f89zewspOQsoddB3P8uqdkuqOk6xf0adaEmkswqndslkco2vPkbdaRD3/8acqI2vOApndodfq8ggrhB3f45xPJD3f8onNk+zv8sn90iiL4efKspm9dJ7PssouIol9MqnNkmlM4mkcstpORB2/////9D3P8bdaJC2v86xf0/0v5A1v4zsvQ7yf48yv4xr/FB1/49zv44wfwsouI2uvk+0P40tfY3vPotpeQomNMroN4qnNolj8gkjcQjisA5w/0wrO41ufgvquofgLIxru8pmtcefq8dfKwbdqQ8zP41uPcnldA/1P4ztPUvqOgmkswhhbo3v/w3vfsceKcdeqk6x/4gg7cggrU4wPwwq+wnlM4ih7xB2P8/0f4ysfIup+g0tvcupuYqntwlkcoiib0pmdVB2fdG5/9F4f8/0P42vPoYcJ5D3v9E4PgVZ5ZJ7P9A1vkys948yvY8ze0vqdEnlr42uvpH5vg6xOE1udovq9YtpNKjkjQ+AAAApnRSTlMAv/swqyGH9wsKPP0C2AyWexzx8+d9+kgUnAbEogSh7nBJvFI1tqekamIszYN1XlgYFRLSkA4NCetCM/Tf0rKKdk05HeaysKqZZ0YqFxUQD+Xi3NiyKCcl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", width=200)
    st.title("Snowflake Validation Automation Tool")
    

# Initialize session state
if 'conn' not in st.session_state:
    st.session_state.conn = None
if 'conn_status' not in st.session_state:
    st.session_state.conn_status = "Disconnected"
if 'current_db' not in st.session_state:
    st.session_state.current_db = None

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["🔐 Login", "⎘ Clone", "🔍 Schema Validation", "📊 KPI Validation"])

# ===== LOGIN TAB =====
with tab1:
    st.markdown("### Snowflake Connection")
    
    with st.form("login_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            user = st.text_input("Username", placeholder="your_username")
            password = st.text_input("Password", type="password", placeholder="••••••••")
        
        with col2:
            account = st.text_input("Account", placeholder="account.region")
            warehouse = st.text_input("Warehouse (optional)", placeholder="WAREHOUSE_NAME")
            role = st.text_input("Role (optional)", placeholder="ROLE_NAME")
        
        login_btn = st.form_submit_button("Connect")
    
    if login_btn:
        conn_params = {
            'user': user,
            'password': password,
            'account': account
        }
        if warehouse:
            conn_params['warehouse'] = warehouse
        if role:
            conn_params['role'] = role
            
        st.session_state.conn, st.session_state.conn_status = get_snowflake_connection(user, password, account)
        
    if st.session_state.conn:
        st.success(st.session_state.conn_status)
        
        # Show connection details
        st.markdown("### Connection Details")
        conn_details = {
            "User": user,
            "Account": account,
            "Status": "Connected",
            "Timestamp": datetime.now().isoformat()
        }
        st.json(conn_details)
        
        # Disconnect button
        if st.button("Disconnect"):
            st.session_state.conn, st.session_state.conn_status = disconnect_snowflake(st.session_state.conn)
            st.session_state.conn = None
            st.experimental_rerun()

# ===== CLONE TAB =====
with tab2:
    if st.session_state.conn:
        st.markdown("### Schema Clone")
        
        with st.form("clone_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                source_db = st.selectbox("Source Database", get_databases(st.session_state.conn))
                source_schema = st.selectbox("Source Schema", get_schemas(st.session_state.conn, source_db))
            
            with col2:
                target_schema = st.text_input("Target Schema Name", placeholder="new_schema_name")
            
            clone_btn = st.form_submit_button("Execute Clone")
        
        if clone_btn:
            if not target_schema:
                st.error("❌ Please enter a target schema name")
            else:
                with st.spinner("Cloning schema..."):
                    success, message, df = clone_schema(
                        st.session_state.conn, source_db, source_schema, target_schema
                    )
                    
                    if success:
                        st.success(message)
                        st.dataframe(df)
                        
                        # Show clone details
                        st.markdown("### Clone Details")
                        details = {
                            "source": f"{source_db}.{source_schema}",
                            "target": f"{source_db}.{target_schema}",
                            "timestamp": datetime.now().isoformat(),
                            "status": "success",
                            "tables_cloned": int(df['Cloned Tables'].iloc[0]) if not df.empty else 0
                        }
                        st.json(details)
                    else:
                        st.error(message)
    else:
        st.warning("Please connect to Snowflake first")

# ===== SCHEMA VALIDATION TAB =====
with tab3:
    if st.session_state.conn:
        st.markdown("### Schema Validation")
        
        with st.form("validation_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                val_db = st.selectbox("Database", get_databases(st.session_state.conn))
                schemas = get_schemas(st.session_state.conn, val_db)
            
            with col2:
                val_source_schema = st.selectbox("Source Schema", schemas)
                val_target_schema = st.selectbox("Target Schema", schemas)
            
            validate_btn = st.form_submit_button("Run Validation")
        
        if validate_btn:
            with st.spinner("Running validation..."):
                # Compare tables
                table_diff = compare_table_differences(
                    st.session_state.conn, val_db, val_source_schema, val_target_schema
                )
                
                # Compare columns and data types
                column_diff, datatype_diff = compare_column_differences(
                    st.session_state.conn, val_db, val_source_schema, val_target_schema
                )
                
                # Combine all results into one DataFrame for download
                combined_df = pd.concat([
                    table_diff.assign(Validation_Type="Table Differences"),
                    column_diff.assign(Validation_Type="Column Differences"),
                    datatype_diff.assign(Validation_Type="Data Type Differences")
                ])
                
                # Show results in tabs
                tab_diff, tab_col, tab_type = st.tabs(["Table Differences", "Column Differences", "Data Type Differences"])
                
                with tab_diff:
                    if not table_diff.empty:
                        st.dataframe(table_diff)
                        st.markdown(get_table_download_link(table_diff, "table_differences.csv"), unsafe_allow_html=True)
                    else:
                        st.info("No table differences found")
                
                with tab_col:
                    if not column_diff.empty:
                        st.dataframe(column_diff)
                        st.markdown(get_table_download_link(column_diff, "column_differences.csv"), unsafe_allow_html=True)
                    else:
                        st.info("No column differences found")
                
                with tab_type:
                    if not datatype_diff.empty:
                        st.dataframe(datatype_diff)
                        st.markdown(get_table_download_link(datatype_diff, "datatype_differences.csv"), unsafe_allow_html=True)
                    else:
                        st.info("No data type differences found")
                
                # Download combined report
                if not combined_df.empty:
                    st.markdown("### Download Full Report")
                    st.markdown(get_table_download_link(combined_df, "schema_validation_report.csv"), unsafe_allow_html=True)
                
                st.success("✅ Validation completed successfully!")
    else:
        st.warning("Please connect to Snowflake first")

# ===== KPI VALIDATION TAB =====
with tab4:
    if st.session_state.conn:
        st.markdown("### KPI Validation")
        
        with st.form("kpi_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                kpi_db = st.selectbox("Database", get_databases(st.session_state.conn))
                schemas = get_schemas(st.session_state.conn, kpi_db)
            
            with col2:
                kpi_source_schema = st.selectbox("Source Schema", schemas)
                kpi_target_schema = st.selectbox("Target Schema", schemas)
            
            st.markdown("### Select KPIs to Validate")
            
            # KPI selection checkboxes
            col1, col2, col3 = st.columns(3)
            
            with col1:
                kpi_total_orders = st.checkbox("Total Orders", value=True)
                kpi_total_revenue = st.checkbox("Total Revenue", value=True)
                kpi_avg_order = st.checkbox("Average Order Value", value=True)
            
            with col2:
                kpi_max_order = st.checkbox("Max Order Value", value=True)
                kpi_min_order = st.checkbox("Min Order Value", value=True)
                kpi_completed = st.checkbox("Completed Orders", value=True)
            
            with col3:
                kpi_cancelled = st.checkbox("Cancelled Orders", value=True)
                kpi_april_orders = st.checkbox("Orders in April 2025", value=True)
                kpi_unique_customers = st.checkbox("Unique Customers", value=True)
            
            kpi_validate_btn = st.form_submit_button("Run KPI Validation")
        
        if kpi_validate_btn:
            # Get selected KPIs
            selected_kpis = []
            if kpi_total_orders: selected_kpis.append("Total Orders")
            if kpi_total_revenue: selected_kpis.append("Total Revenue")
            if kpi_avg_order: selected_kpis.append("Average Order Value")
            if kpi_max_order: selected_kpis.append("Max Order Value")
            if kpi_min_order: selected_kpis.append("Min Order Value")
            if kpi_completed: selected_kpis.append("Completed Orders")
            if kpi_cancelled: selected_kpis.append("Cancelled Orders")
            if kpi_april_orders: selected_kpis.append("Orders in April 2025")
            if kpi_unique_customers: selected_kpis.append("Unique Customers")
            
            if not selected_kpis:
                st.warning("⚠️ No KPIs selected for validation")
            else:
                with st.spinner("Running KPI validation..."):
                    df, message = validate_kpis(
                        st.session_state.conn, kpi_db, kpi_source_schema, kpi_target_schema, selected_kpis
                    )
                    
                    if not df.empty:
                        st.dataframe(df)
                        st.markdown(get_table_download_link(df, "kpi_validation_report.csv"), unsafe_allow_html=True)
                        st.success(message)
                    else:
                        st.error(message)
    else:
        st.warning("Please connect to Snowflake first")