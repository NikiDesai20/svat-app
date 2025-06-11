# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime
import re
import io
import time
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

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
            return pd.DataFrame(), "⚠️ No matching KPIs found in ORDER_KPIS table."

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
                if isinstance(result_source, (int, float)) and isinstance(result_clone, (int, float)):
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

# ========== STREAMLIT UI ==========
def main():
    # Set page config
    st.set_page_config(
        page_title="Snowflake Validation Automation Tool",
        page_icon="❄️",
        layout="wide"
    )
    
    # Add company logo at the top
    st.image("data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAcYAAABCCAMAAAD66oZhAAAC91BMVEUAAAD///////////////////////////////////////////////////////////////////////////////////////////////////////////9C3P8fndX///////////////////////////////////////////////////////////////////////////////////////////////8fisgwxfr///////////9C3P////8adaL///////81u/n///////85zv1D3P86xv3///////////////////////8dhbg6zv7///850P8+0P////////8adaM2vfv///8/1v8egbH///8ztfY3v/s/0vv///9C2/v///8wqutB2v4adaI+0f4dfKowre8pns47yv3///////////////8vqutC2vsdfq8xsO7///8iib8/0/85xPk6yPIztfX///84xfkZdKUztvcadKJC3PNB2P8iib8+0f4ceKZB2f9C2/9C3f89zewspOQsoddB3P8uqdkuqOk6xf0adaEmkswqndslkco2vPkbdaRD3/8acqI2vOApndodfq8ggrhB3f45xPJD3f8onNk+zv8sn90iiL4efKspm9dJ7PssouIol9MqnNkmlM4mkcstpORB2/////9D3P8bdaJC2v86xf0/0v5A1v4zsvQ7yf48yv4xr/FB1/49zv44wfwsouI2uvk+0P40tfY3vPotpeQomNMroN4qnNolj8gkjcQjisA5w/0wrO41ufgvquofgLIxru8pmtcefq8dfKwbdqQ8zP41uPcnldA/1P4ztPUvqOgmkswhhbo3v/w3vfsceKcdeqk6x/4gg7cggrU4wPwwq+wnlM4ih7xB2P8/0f4ysfIup+g0tvcupuYqntwlkcoiib0pmdVB2fdG5/9F4f8/0P42vPoYcJ5D3v9E4PgVZ5ZJ7P9A1vkys948yvY8ze0vqdEnlr42uvpH5vg6xOE1udovq9YtpNKjkjQ+AAAApnRSTlMAv/swqyGH9wsKPP0C2AyWexzx8+d9+kgUnAbEogSh7nBJvFI1tqekamIszYN1XlgYFRLSkA4NCetCM/Tf0rKKdk05HeaysKqZZ0YqFxUQD+Xi3NiyKCcl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", width=200)
    st.title("Snowflake Validation Automation Tool")
    
    # Initialize session state
    if 'conn' not in st.session_state:
        st.session_state.conn = None
    if 'current_db' not in st.session_state:
        st.session_state.current_db = None
    if 'login_success' not in st.session_state:
        st.session_state.login_success = False
    
    # ===== LOGIN SECTION =====
    with st.expander("🔐 Login", expanded=not st.session_state.login_success):
        with st.form("login_form"):
            st.subheader("Snowflake Connection")
            user = st.text_input("Username", placeholder="your_username")
            password = st.text_input("Password", type="password", placeholder="••••••••")
            account = st.text_input("Account", placeholder="account.region")
            
            login_btn = st.form_submit_button("Connect")
            
            if login_btn:
                with st.spinner("Connecting to Snowflake..."):
                    st.session_state.conn, msg = get_snowflake_connection(user, password, account)
                    if st.session_state.conn:
                        st.session_state.login_success = True
                        st.session_state.conn_details = {"user": user, "account": account}
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
    
    # Display connection details if logged in
    if st.session_state.login_success:
        st.sidebar.subheader("Connection Details")
        st.sidebar.json(st.session_state.conn_details)
        
        if st.sidebar.button("Disconnect"):
            with st.spinner("Disconnecting..."):
                st.session_state.conn, msg = disconnect_snowflake(st.session_state.conn)
                st.session_state.login_success = False
                st.session_state.conn_details = {}
                st.sidebar.success(msg)
                st.rerun()
    
    # ===== MAIN APP =====
    if st.session_state.login_success:
        # Create tabs
        tab1, tab2, tab3 = st.tabs(["⎘ Clone", "🔍 Schema Validation", "📊 KPI Validation"])
        
        # ===== CLONE SECTION =====
        with tab1:
            st.subheader("Schema Clone")
            col1, col2 = st.columns(2)
            
            with col1:
                # Get databases if not already loaded
                if 'databases' not in st.session_state:
                    st.session_state.databases = get_databases(st.session_state.conn)
                
                source_db = st.selectbox(
                    "Source Database",
                    st.session_state.databases,
                    key="clone_source_db"
                )
                
                # Get schemas for selected database
                source_schemas = get_schemas(st.session_state.conn, source_db)
                source_schema = st.selectbox(
                    "Source Schema",
                    source_schemas,
                    key="clone_source_schema"
                )
                
                target_schema = st.text_input(
                    "Target Schema Name",
                    key="clone_target_schema"
                )
                
                if st.button("Execute Clone", key="clone_execute"):
                    if not target_schema:
                        st.error("❌ Please enter a target schema name")
                    else:
                        with st.spinner(f"Cloning {source_db}.{source_schema} to {source_db}.{target_schema}..."):
                            success, message, df = clone_schema(
                                st.session_state.conn, source_db, source_schema, target_schema
                            )
                            
                            if success:
                                st.success(message)
                                st.dataframe(df)
                            else:
                                st.error(message)
            
            with col2:
                st.markdown("### Clone Status")
                if 'clone_status' in st.session_state:
                    if st.session_state.clone_status.startswith("✅"):
                        st.success(st.session_state.clone_status)
                    else:
                        st.error(st.session_state.clone_status)
                
                st.markdown("### Clone Details")
                if 'clone_details' in st.session_state:
                    st.json(st.session_state.clone_details)
        
        # ===== SCHEMA VALIDATION SECTION =====
        with tab2:
            st.subheader("Schema Validation")
            col1, col2 = st.columns([1, 2])
            
            with col1:
                # Get databases if not already loaded
                if 'val_databases' not in st.session_state:
                    st.session_state.val_databases = get_databases(st.session_state.conn)
                
                val_db = st.selectbox(
                    "Database",
                    st.session_state.val_databases,
                    key="val_db"
                )
                
                # Get schemas for selected database
                val_schemas = get_schemas(st.session_state.conn, val_db)
                val_source_schema = st.selectbox(
                    "Source Schema",
                    val_schemas,
                    key="val_source_schema"
                )
                val_target_schema = st.selectbox(
                    "Target Schema",
                    val_schemas,
                    key="val_target_schema"
                )
                
                if st.button("Run Validation", key="val_execute"):
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
                        
                        st.session_state.val_results = {
                            "table_diff": table_diff,
                            "column_diff": column_diff,
                            "datatype_diff": datatype_diff,
                            "combined": combined_df
                        }
                        
                        st.success("✅ Validation completed successfully!")
            
            with col2:
                if 'val_results' in st.session_state:
                    tab1, tab2, tab3 = st.tabs(["Table Differences", "Column Differences", "Data Type Differences"])
                    
                    with tab1:
                        st.dataframe(st.session_state.val_results["table_diff"])
                    
                    with tab2:
                        st.dataframe(st.session_state.val_results["column_diff"])
                    
                    with tab3:
                        st.dataframe(st.session_state.val_results["datatype_diff"])
                    
                    # Download button
                    if not st.session_state.val_results["combined"].empty:
                        csv = st.session_state.val_results["combined"].to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Schema Report",
                            data=csv,
                            file_name=f"schema_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime='text/csv'
                        )
        
        # ===== KPI VALIDATION SECTION =====
with tab3:
    st.subheader("KPI Validation")
    
    # Get databases if not already loaded
    if 'kpi_databases' not in st.session_state:
        st.session_state.kpi_databases = get_databases(st.session_state.conn)
    
    kpi_db = st.selectbox(
        "Database",
        st.session_state.kpi_databases,
        key="kpi_db"
    )
    
    # Get schemas for selected database
    kpi_schemas = get_schemas(st.session_state.conn, kpi_db)
    kpi_source_schema = st.selectbox(
        "Source Schema",
        kpi_schemas,
        key="kpi_source_schema"
    )
    kpi_target_schema = st.selectbox(
        "Target Schema",
        kpi_schemas,
        key="kpi_target_schema"
    )
    
    st.markdown("### Select KPIs to Validate")
    
    # Select All checkbox
    select_all = st.checkbox("Select All", value=True, key="kpi_select_all")
    
    # Create a container for the KPI checkboxes
    kpi_container = st.container()
    
    # KPI checkboxes - now in a single row without nested columns
    with kpi_container:
        kpi_total_orders = st.checkbox("Total Orders", value=select_all, key="kpi_total_orders")
        kpi_total_revenue = st.checkbox("Total Revenue", value=select_all, key="kpi_total_revenue")
        kpi_avg_order = st.checkbox("Average Order Value", value=select_all, key="kpi_avg_order")
        kpi_max_order = st.checkbox("Max Order Value", value=select_all, key="kpi_max_order")
        kpi_min_order = st.checkbox("Min Order Value", value=select_all, key="kpi_min_order")
        kpi_completed = st.checkbox("Completed Orders", value=select_all, key="kpi_completed")
        kpi_cancelled = st.checkbox("Cancelled Orders", value=select_all, key="kpi_cancelled")
        kpi_april_orders = st.checkbox("Orders in April 2025", value=select_all, key="kpi_april_orders")
        kpi_unique_customers = st.checkbox("Unique Customers", value=select_all, key="kpi_unique_customers")
    
    if st.button("Run KPI Validation", key="kpi_execute"):
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
                    st.session_state.conn,
                    kpi_db,
                    kpi_source_schema,
                    kpi_target_schema,
                    selected_kpis
                )
                
                if message.startswith("✅"):
                    st.success(message)
                else:
                    st.error(message)
                
                st.session_state.kpi_results = df
    
    if 'kpi_results' in st.session_state:
        st.dataframe(st.session_state.kpi_results)
        
        # Download button
        if not st.session_state.kpi_results.empty:
            csv = st.session_state.kpi_results.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download KPI Report",
                data=csv,
                file_name=f"kpi_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime='text/csv'
            )               
                   

if __name__ == "__main__":
    main()