import pymysql
import pymongo

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB = "retail_ecommerce"

MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
MONGODB_USER = "root"
MONGODB_PASSWORD = "root"
MONGODB_DB = "retail_ecommerce"


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

def get_mongodb_conn():
    return pymongo.MongoClient(
        host=MONGODB_HOST,
        port=MONGODB_PORT,
        username=MONGODB_USER,
        password=MONGODB_PASSWORD
    )   

def init_mysql_db(mysql_cursor, mysql_conn):
    # CREATE DATABASE
    create_db_query = f"""CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"""
    mysql_cursor.execute(create_db_query)
    print(f"Database {MYSQL_DB} created successfully")

    # CREATE TABLES
    # 1. Customer
    create_customer_table_query = """
        CREATE TABLE retail_ecommerce.customers (
            customer_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            customer_uuid VARCHAR(36) UNIQUE NOT NULL,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            phone VARCHAR(20),
            date_of_birth DATE,
            gender ENUM('M', 'F', 'O'),
            address_line1 VARCHAR(255),
            address_line2 VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            postal_code VARCHAR(20),
            customer_segment ENUM('BRONZE', 'SILVER', 'GOLD', 'PLATINUM'),
            loyalty_points INT DEFAULT 0,
            total_spent DECIMAL(15,2) DEFAULT 0.00,
            registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login_date TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_email (email),
            INDEX idx_customer_segment (customer_segment),
            INDEX idx_registration_date (registration_date)
        );
    """
    # 2. Product
    create_product_table_query = """
        CREATE TABLE retail_ecommerce.products (
            product_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            product_uuid VARCHAR(36) UNIQUE NOT NULL,
            sku VARCHAR(100) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            brand VARCHAR(100),
            category_id BIGINT,
            subcategory_id BIGINT,
            price DECIMAL(10,2) NOT NULL,
            cost_price DECIMAL(10,2),
            weight_kg DECIMAL(8,3),
            dimensions_cm VARCHAR(50),
            color VARCHAR(50),
            size VARCHAR(20),
            material VARCHAR(100),
            is_active BOOLEAN DEFAULT TRUE,
            stock_quantity INT DEFAULT 0,
            reorder_level INT DEFAULT 10,
            supplier_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_sku (sku),
            INDEX idx_category (category_id),
            INDEX idx_brand (brand),
            INDEX idx_is_active (is_active)
        );
    """
    # 3. Category

    # 4. Payment
    # 5. Review
    # 6. Category
    # 7. Subcategory

def init_mongodb_db(mongodb_cursor, mongodb_conn):
    pass

def main():
    mysql_conn = get_mysql_conn()
    mongodb_conn = get_mongodb_conn()
    mysql_cursor = mysql_conn.cursor()
    mongodb_cursor = mongodb_conn.cursor()
    init_mysql_db(mysql_cursor, mysql_conn)
    init_mongodb_db(mongodb_cursor, mongodb_conn)

if __name__ == "__main__":
    main()