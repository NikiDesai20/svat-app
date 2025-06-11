# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime
import re
import io
import warnings

# Suppress Streamlit warnings
warnings.filterwarnings("ignore", category=UserWarning)

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

# ========== STREAMLIT UI ==========
def main():
    # Initialize session state
    if 'conn' not in st.session_state:
        st.session_state.conn = None
    if 'current_db' not in st.session_state:
        st.session_state.current_db = None
    if 'login_success' not in st.session_state:
        st.session_state.login_success = False

    # Add company logo at the top with error handling
    try:
        # Using a placeholder URL - replace with your actual logo URL or path
        st.image("https://via.placeholder.com/200x50?text=My+Logo", width=200)
    except Exception as e:
        st.warning(f"Could not load logo: {str(e)}")

    st.title("Snowflake Validation Automation Tool")

    # ===== LOGIN SECTION =====
    with st.expander("🔐 Login", expanded=not st.session_state.login_success):
        with st.form("login_form"):
            st.markdown("### Snowflake Connection")
            user = st.text_input("Username", placeholder="your_username")
            password = st.text_input("Password", type="password", placeholder="••••••••")
            account = st.text_input("Account", placeholder="account.region")

            login_btn = st.form_submit_button("Connect")

        if login_btn:
            with st.spinner("Connecting to Snowflake..."):
                conn, msg = get_snowflake_connection(user, password, account)
                if conn:
                    st.session_state.conn = conn
                    st.session_state.login_success = True
                    st.session_state.conn_details = {"user": user, "account": account}
                    st.success(msg)
                    st.experimental_rerun()
                else:
                    st.error(msg)

    # Display connection details if logged in
    if st.session_state.login_success:
        st.sidebar.markdown("### Connection Details")
        st.sidebar.json(st.session_state.conn_details)
        
        if st.sidebar.button("Disconnect"):
            with st.spinner("Disconnecting..."):
                st.session_state.conn, msg = disconnect_snowflake(st.session_state.conn)
                st.session_state.login_success = False
                st.session_state.conn_details = {}
                st.sidebar.success(msg)
                st.experimental_rerun()

        # ===== CLONE SECTION =====
        st.markdown("## ⎘ Schema Clone")
        with st.form("clone_form"):
            st.markdown("### Source Selection")
            databases = get_databases(st.session_state.conn)
            source_db = st.selectbox("Source Database", databases)
            
            schemas = get_schemas(st.session_state.conn, source_db)
            source_schema = st.selectbox("Source Schema", schemas)
            
            target_schema = st.text_input("Target Schema Name")
            
            clone_btn = st.form_submit_button("Execute Clone")

        if clone_btn:
            with st.spinner("Cloning schema..."):
                success, message, df = clone_schema(
                    st.session_state.conn, source_db, source_schema, target_schema
                )
                
                if success:
                    st.success(message)
                    st.dataframe(df)
                else:
                    st.error(message)

        # ===== SCHEMA VALIDATION SECTION =====
        st.markdown("## 🔍 Schema Validation")
        with st.form("validation_form"):
            st.markdown("### Validation Configuration")
            val_db = st.selectbox("Database", databases, key="val_db")
            
            val_schemas = get_schemas(st.session_state.conn, val_db)
            val_source_schema = st.selectbox("Source Schema", val_schemas, key="val_source_schema")
            val_target_schema = st.selectbox("Target Schema", val_schemas, key="val_target_schema")
            
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
                
                st.success("✅ Validation completed successfully!")
                
                # Display results in tabs
                tab1, tab2, tab3 = st.tabs(["Table Differences", "Column Differences", "Data Type Differences"])
                
                with tab1:
                    st.dataframe(table_diff)
                
                with tab2:
                    st.dataframe(column_diff)
                
                with tab3:
                    st.dataframe(datatype_diff)
                
                # Download button
                if not combined_df.empty:
                    csv = combined_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Schema Report",
                        data=csv,
                        file_name=f"schema_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime='text/csv'
                    )

        # ===== KPI VALIDATION SECTION =====
        st.markdown("## 📊 KPI Validation")
        with st.form("kpi_form"):
            st.markdown("### KPI Configuration")
            kpi_db = st.selectbox("Database", databases, key="kpi_db")
            
            kpi_schemas = get_schemas(st.session_state.conn, kpi_db)
            kpi_source_schema = st.selectbox("Source Schema", kpi_schemas, key="kpi_source_schema")
            kpi_target_schema = st.selectbox("Target Schema", kpi_schemas, key="kpi_target_schema")
            
            # KPI Selection Checkboxes
            st.markdown("### Select KPIs to Validate")
            kpi_select_all = st.checkbox("Select All", value=True, key="kpi_select_all")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                kpi_total_orders = st.checkbox("Total Orders", value=kpi_select_all, key="kpi_total_orders")
                kpi_total_revenue = st.checkbox("Total Revenue", value=kpi_select_all, key="kpi_total_revenue")
                kpi_avg_order = st.checkbox("Average Order Value", value=kpi_select_all, key="kpi_avg_order")
            with col2:
                kpi_max_order = st.checkbox("Max Order Value", value=kpi_select_all, key="kpi_max_order")
                kpi_min_order = st.checkbox("Min Order Value", value=kpi_select_all, key="kpi_min_order")
                kpi_completed = st.checkbox("Completed Orders", value=kpi_select_all, key="kpi_completed")
            with col3:
                kpi_cancelled = st.checkbox("Cancelled Orders", value=kpi_select_all, key="kpi_cancelled")
                kpi_april_orders = st.checkbox("Orders in April 2025", value=kpi_select_all, key="kpi_april_orders")
                kpi_unique_customers = st.checkbox("Unique Customers", value=kpi_select_all, key="kpi_unique_customers")
            
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
                    df, msg = validate_kpis(
                        st.session_state.conn, kpi_db, kpi_source_schema, kpi_target_schema, selected_kpis
                    )
                    
                    if msg.startswith("✅"):
                        st.success(msg)
                        st.dataframe(df)
                        
                        # Download button
                        csv = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download KPI Report",
                            data=csv,
                            file_name=f"kpi_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime='text/csv'
                        )
                    else:
                        st.error(msg)

if __name__ == "__main__":
    main()