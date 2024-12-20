import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import pandas as pd
from sqlalchemy import create_engine, text, event
import json
import os
from dotenv import load_dotenv

# --------------------------------------------------------------------------------
# Load Environment Variables
# --------------------------------------------------------------------------------
load_dotenv()  # Load environment variables from .env file



DB_HOST = os.getenv("DB_HOST","localhost")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default PostgreSQL port
DB_NAME = os.getenv("DB_NAME","snowflakedb")
DB_USER = os.getenv("DB_USER","snow")
DB_PASSWORD = os.getenv("DB_PASSWORD","snowflake1234")

SNOWFLAKE_CONNECTION_URL = os.getenv("SNOWFLAKE_CONNECTION_URL",'snowflake://SNOWFLAKE_USER:SNOWFLAKE_PASS_OR_TOKEN_OR_KEY:postgres_role:postgres_encrypt_wh:SNOWFLAKE_ACCOUNT@postgresschema/postgresdb')

# --------------------------------------------------------------------------------
# Database Connection Setup with SQLAlchemy Event Listener
# --------------------------------------------------------------------------------

# Create SQLAlchemy engine
try:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
except Exception as e:
    st.error(f"Error creating the database engine: {e}")
    st.stop()

# Define the SET command
SET_SNOWFLAKE_CONNECTION_URL = f"SET my.snowflake_connection_url = '{SNOWFLAKE_CONNECTION_URL}';"

# Event listener to execute the SET command upon each new connection
@event.listens_for(engine, "connect")
def set_postgres_environment_variables(dbapi_connection, connection_record):
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute(SET_SNOWFLAKE_CONNECTION_URL)
        cursor.close()
    except Exception as e:
        st.error(f"Error setting PostgreSQL environment variable: {e}")

# --------------------------------------------------------------------------------
# Helper Functions for DB Operations
# --------------------------------------------------------------------------------

def get_all_employees():
    """Fetches all rows from the snowflake_employee view."""
    query = "SELECT * FROM snowflake_employee;"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error fetching employee data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def insert_employee(emp_id, firstname, lastname, address, postalcode, phone):
    """Inserts a new employee using the UDF snowflake_employee_insert_aes."""
    query = text("""
        SELECT snowflake_employee_insert_aes(:emp_id, :firstname, :lastname, :address, :postalcode, :phone);
    """)
    try:
        with engine.connect() as conn:
            conn.execute(query, {
                'emp_id': emp_id,
                'firstname': firstname,
                'lastname': lastname,
                'address': address,
                'postalcode': postalcode,
                'phone': phone
            })
    except Exception as e:
        st.error(f"Error inserting employee: {e}")

def update_employee(emp_id, updates: dict):
    """Updates an existing employee using snowflake_employee_update_aes.
       The updates parameter should be a dictionary of fields to update."""
    json_str = json.dumps(updates)
    query = text("""
        SELECT snowflake_employee_update_aes(:emp_id, :json_updates);
    """)
    try:
        with engine.connect() as conn:
            conn.execute(query, {'emp_id': emp_id, 'json_updates': json_str})
    except Exception as e:
        st.error(f"Error updating employee: {e}")

def delete_employee(emp_id):
    """Deletes an employee using snowflake_employee_aes_delete."""
    query = text("SELECT snowflake_employee_aes_delete(:emp_id);")
    try:
        with engine.connect() as conn:
            conn.execute(query, {'emp_id': emp_id})
    except Exception as e:
        st.error(f"Error deleting employee: {e}")

# --------------------------------------------------------------------------------
# Streamlit App
# --------------------------------------------------------------------------------
st.set_page_config(page_title="Snowflake Employee Management", layout="wide")
st.title("❄️ Snowflake Employee Management")

# Fetch data
df = get_all_employees()

st.subheader("📋 Employee Table")
st.write("Below is the decrypted employee data from the PostgreSQL view.")

# Check if DataFrame is not empty
if not df.empty:
    # Set up AgGrid options
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection('single', use_checkbox=True)  # Allow single row selection
    gb.configure_pagination(enabled=True, paginationAutoPageSize=True)  # Enable pagination
    gb.configure_side_bar()  # Enable sidebar for additional options
    gb.configure_default_column(editable=False, sortable=True, filter=True)  # Make columns sortable and filterable
    grid_options = gb.build()

    # Display the grid
    grid_response = AgGrid(
        df,
        gridOptions=grid_options,
        height=400,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=False  # Set to True to enable JS functions, but it's safer to keep False
    )

    selected_rows = grid_response["selected_rows"]

    # Handle selection based on the type of selected_rows
    selected_emp_id = None
    if isinstance(selected_rows, list):
        if len(selected_rows) > 0:
            selected_emp_id = selected_rows[0].get("emp_id")
    elif isinstance(selected_rows, pd.DataFrame):
        if not selected_rows.empty:
            selected_emp_id = selected_rows.iloc[0].get("emp_id")
    # You can add more type checks if necessary

    # --------------------------------------------------------------------------------
    # Insert Form
    # --------------------------------------------------------------------------------
    st.markdown("---")
    st.subheader("➕ Insert a New Employee")
    with st.form("insert_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_emp_id = st.text_input("**Employee ID**", max_chars=10)
            new_firstname = st.text_input("**First Name**")
            new_lastname = st.text_input("**Last Name**")
        with col2:
            new_address = st.text_input("**Address**")
            new_postalcode = st.text_input("**Postal Code**")
            new_phone = st.text_input("**Phone**")
        
        insert_submit = st.form_submit_button("Insert Employee")
        if insert_submit:
            if all([new_emp_id, new_firstname, new_lastname]):
                insert_employee(new_emp_id, new_firstname, new_lastname, new_address, new_postalcode, new_phone)
                st.success("✅ Employee inserted successfully.")
                st.experimental_rerun()  # Refresh data
            else:
                st.error("❌ **Employee ID**, **First Name**, and **Last Name** are required fields.")

    # --------------------------------------------------------------------------------
    # Update Form
    # --------------------------------------------------------------------------------
    st.markdown("---")
    st.subheader("✏️ Update Selected Employee")
    if selected_emp_id:
        st.write(f"**Selected Employee ID:** `{selected_emp_id}`")
        
        # Fetch current details of the selected employee
        try:
            with engine.connect() as conn:
                current_details_query = text("SELECT * FROM snowflake_employee WHERE emp_id = :emp_id;")
                current_details_df = pd.read_sql(current_details_query, conn, params={"emp_id": selected_emp_id})
        except Exception as e:
            st.error(f"Error fetching current employee details: {e}")
            current_details_df = pd.DataFrame()
        
        if not current_details_df.empty:
            current_details = current_details_df.iloc[0].to_dict()
            st.write("**Current Details:**")
            st.json(current_details)

            with st.form("update_form", clear_on_submit=True):
                col1, col2 = st.columns(2)
                with col1:
                    upd_firstname = st.text_input("**New First Name** (leave blank if no change)", value="")
                    upd_lastname = st.text_input("**New Last Name** (leave blank if no change)", value="")
                with col2:
                    upd_address = st.text_input("**New Address** (leave blank if no change)", value="")
                    upd_postalcode = st.text_input("**New Postal Code** (leave blank if no change)", value="")
                    upd_phone = st.text_input("**New Phone** (leave blank if no change)", value="")
                
                update_submit = st.form_submit_button("Update Employee")
                if update_submit:
                    updates = {}
                    if upd_firstname:
                        updates["firstname"] = upd_firstname
                    if upd_lastname:
                        updates["lastname"] = upd_lastname
                    if upd_address:
                        updates["address"] = upd_address
                    if upd_postalcode:
                        updates["postalcode"] = upd_postalcode
                    if upd_phone:
                        updates["phone"] = upd_phone
                    
                    if updates:
                        update_employee(selected_emp_id, updates)
                        st.success("✅ Employee updated successfully.")
                        st.experimental_rerun()  # Refresh data
                    else:
                        st.info("ℹ️ No changes submitted.")
        else:
            st.warning("⚠️ Selected employee not found.")
    else:
        st.info("📌 Select an employee from the table to update.")

    # --------------------------------------------------------------------------------
    # Delete Button
    # --------------------------------------------------------------------------------
    st.markdown("---")
    st.subheader("🗑️ Delete Selected Employee")
    if selected_emp_id:
        delete_confirm = st.checkbox("⚠️ **Are you sure you want to delete this employee?** This action cannot be undone.")
        if delete_confirm:
            if st.button("Delete Employee"):
                delete_employee(selected_emp_id)
                st.success("✅ Employee deleted successfully.")
                st.experimental_rerun()  # Refresh data
    else:
        st.info("📌 Select an employee from the table to delete.")

else:
    st.warning("⚠️ No employee data available to display.")

# --------------------------------------------------------------------------------
# Footer
# --------------------------------------------------------------------------------
st.markdown("---")
st.markdown("Developed with ❤️ using Streamlit and PostgreSQL.")
