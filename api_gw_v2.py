from flask import Flask, jsonify
from flask_httpauth import HTTPBasicAuth
import mysql.connector
import os

# Initialize the Flask app
app = Flask(__name__)
auth = HTTPBasicAuth()

# Set the API username and password for Basic Authentication
app.config['BASIC_AUTH_USERNAME'] = os.getenv('BASIC_AUTH_USERNAME', 'ioc_bdu_api')
app.config['BASIC_AUTH_PASSWORD'] = os.getenv('BASIC_AUTH_PASSWORD', '*Bdu@apigw2024')

# Set the database connection details from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '192.168.69.26'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '*Bdu@mysqlapi2024'),
    'database': os.getenv('DB_NAME', 'IOC_BDU_ODP')
}

# Verify password function to check against API credentials
@auth.verify_password
def verify_password(username, password):
    return username == app.config['BASIC_AUTH_USERNAME'] and password == app.config['BASIC_AUTH_PASSWORD']

# Function to fetch data from MySQL database
def get_data_from_db(table_name):
    try:
        # Establish database connection
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # SQL query to fetch data from the specified table
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        
        # Fetch all rows of the query result
        rows = cursor.fetchall()
        
        # Close the database connection
        cursor.close()
        conn.close()
        
        return rows
    except mysql.connector.Error as err:
        # Log the error and raise a custom error message
        app.logger.error(f"Database error: {err}")
        return None

# Dynamic route that fetches data from any table
@app.route('/data/<table_name>', methods=['GET'])
@auth.login_required  # Protect this route with authentication
def get_data(table_name):
    # List of valid tables (for security reasons, ensure the table name is valid)
    valid_tables = ['dim_bang_diem_odp', 'dim_chuyen_nganh_odp', 'dim_danh_sach_diem_danh_odp', 'dim_giang_vien_odp', 
                    'dim_he_dao_tao_odp', 'dim_khoa_odp', 'dim_lop_odp', 'dim_mon_dang_ky_odp', 'dim_mon_hoc_odp', 
                    'diem_nganh_odp', 'fact_ho_so_sinh_vien_odp', 'fact_nhom_hoc_odp']
    
    # Check if the table name is valid
    if table_name not in valid_tables:
        return jsonify({"error": "Invalid table name"}), 400
    
    # Fetch data from the database
    data = get_data_from_db(table_name)
    
    # Check if data retrieval was successful
    if data is None:
        return jsonify({"error": "Failed to retrieve data from the database"}), 500
    
    # Return data as JSON
    return jsonify(data)

if __name__ == '__main__':
    # Run the Flask app on port 5001 (change the port if needed)
    app.run(host='0.0.0.0', port=5001, debug=True)