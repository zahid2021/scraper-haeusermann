import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict, Any, List
import os
from datetime import date

# ==============================
# DATABASE CONNECTION
# ==============================

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=3306,
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE'),
        )
        return connection
    except Error as e:
        print(f"✗ Error connecting to MySQL: {e}")
        return None


# ==============================
# CREATE TABLES (NEW SCHEMA)
# ==============================

def create_tables(connection):
    cursor = connection.cursor()

    # ---------------- Companies ----------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) UNIQUE,
        industry VARCHAR(100),
        company_type ENUM('employer','recruiter') DEFAULT 'employer',
        company_size ENUM('micro','small','medium','large','enterprise','unknown') DEFAULT 'unknown',
        website TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # ---------------- Job Categories ----------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS job_categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        main_category VARCHAR(100),
        sub_category VARCHAR(150)
    )
    """)

    # ---------------- Regions ----------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS regions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country VARCHAR(100),
        state VARCHAR(100),
        city VARCHAR(100),
        postal_code VARCHAR(20)
    )
    """)

    # ---------------- Jobs ----------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INT AUTO_INCREMENT PRIMARY KEY,

        -- Manual fields (NOT from LLM)
        job_external_id VARCHAR(100),
        title VARCHAR(255),
        job_link TEXT,
        job_source VARCHAR(100),
        description TEXT,

        -- LLM fields
        summary TEXT,
        company_id INT,
        category_id INT,
        region_id INT,

        seniority_level ENUM('intern','junior','mid','senior','lead','manager','unknown') DEFAULT 'unknown',
        experience_min_years INT DEFAULT 0,
        experience_max_years INT DEFAULT 0,

        employment_type ENUM('full-time','part-time','contract','internship','temporary','unknown') DEFAULT 'unknown',

        workload_min INT,
        workload_max INT,

        remote_type ENUM('on-site','hybrid','remote','unknown') DEFAULT 'unknown',

        management_responsibility BOOLEAN DEFAULT FALSE,
        home_office_possible BOOLEAN DEFAULT FALSE,
        education_level VARCHAR(150),

        published_at DATE,          
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL,
        FOREIGN KEY (category_id) REFERENCES job_categories(id) ON DELETE SET NULL,
        FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE SET NULL,

        INDEX idx_seniority (seniority_level),
        INDEX idx_employment (employment_type),
        INDEX idx_remote (remote_type)
    )
    """)

    # ---------------- Skills ----------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS skills (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(150) UNIQUE
    )
    """)

    # ---------------- Job Skills ----------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS job_skills (
        job_id INT,
        skill_id INT,
        skill_type ENUM('required','preferred'),
        PRIMARY KEY (job_id, skill_id),
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
        FOREIGN KEY (skill_id) REFERENCES skills(id) ON DELETE CASCADE
    )
    """)

    connection.commit()
    cursor.close()
    print("✓ All 6 tables created successfully")


# ==============================
# HELPER GET OR CREATE METHODS
# ==============================

def get_or_create_company(connection, company_data: Dict[str, Any]) -> Optional[int]:
    cursor = connection.cursor()

    cursor.execute("SELECT id FROM companies WHERE name = %s", (company_data.get("name"),))
    result = cursor.fetchone()
    if result:
        return result[0]

    if company_data.get("company_size") == '':
        company_data["company_size"] = "unknown"
    cursor.execute("""
        INSERT INTO companies (name, industry, company_type, company_size)
        VALUES (%s, %s, %s, %s)
    """, (
        company_data.get("name"),
        company_data.get("industry"),
        company_data.get("company_type", "employer"),
        company_data.get("company_size","unknown")
    ))

    connection.commit()
    return cursor.lastrowid


def get_or_create_category(connection, category_data: Dict[str, Any]) -> Optional[int]:
    cursor = connection.cursor()

    cursor.execute("""
        SELECT id FROM job_categories
        WHERE main_category=%s AND sub_category=%s
    """, (
        category_data.get("main_category"),
        category_data.get("sub_category")
    ))

    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO job_categories (main_category, sub_category)
        VALUES (%s, %s)
    """, (
        category_data.get("main_category"),
        category_data.get("sub_category")
    ))

    connection.commit()
    return cursor.lastrowid


def get_or_create_region(connection, location_data: Dict[str, Any]) -> Optional[int]:
    cursor = connection.cursor()

    cursor.execute("""
        SELECT id FROM regions
        WHERE country=%s AND state=%s AND city=%s
    """, (
        location_data.get("country"),
        location_data.get("state"),
        location_data.get("city")
    ))

    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO regions (country, state, city, postal_code)
        VALUES (%s, %s, %s, %s)
    """, (
        location_data.get("country"),
        location_data.get("state"),
        location_data.get("city"),
        location_data.get("postal_code")
    ))

    connection.commit()
    return cursor.lastrowid


def get_or_create_skill(connection, skill_name: str) -> int:
    cursor = connection.cursor()

    cursor.execute("SELECT id FROM skills WHERE name=%s", (skill_name,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("INSERT INTO skills (name) VALUES (%s)", (skill_name,))
    connection.commit()
    return cursor.lastrowid


# ==============================
# INSERT JOB (FULL NORMALIZED)
# ==============================

def insert_job(
    connection,
    job_id: str,
    job_title: str,
    job_link: str,
    job_source: str,
    job_description: str,
    parsed_data: Dict[str, Any]
) -> bool:

    try:
        cursor = connection.cursor()

        # Check duplicate
        cursor.execute("SELECT id FROM jobs WHERE job_external_id=%s", (job_id,))
        if cursor.fetchone() and job_id not in ["", None]:
            print("⊘ Job already exists")
            return False

        try:
            # if any result pending 
            cursor.fetchall()
        except Exception:
            pass    
        # Normalized relations
        company_id = get_or_create_company(connection, parsed_data.get("company", {}))
        category_id = get_or_create_category(connection, parsed_data.get("category", {}))
        region_id = get_or_create_region(connection, parsed_data.get("location", {}))

        if parsed_data.get('seniority_level') == '':
            parsed_data['seniority_level'] = 'unknown'
        if parsed_data.get('employment_type') == '':
            parsed_data['employment_type'] = 'unknown'
        if parsed_data.get('remote_type') == '':
            parsed_data['remote_type'] = 'unknown'
        if parsed_data.get('published_at') == '':
            parsed_data['published_at'] = None          
        # Insert Job
        cursor.execute("""
        INSERT INTO jobs (
            job_external_id, title, job_link, job_source, description,
            summary, company_id, category_id, region_id,
            seniority_level, experience_min_years, experience_max_years,
            employment_type, workload_min, workload_max,
            remote_type, management_responsibility,
            home_office_possible, education_level, published_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            job_id,
            job_title,
            job_link,
            job_source,
            job_description,
            parsed_data.get("summary"),
            company_id,
            category_id,
            region_id,
            parsed_data.get("seniority_level"),
            parsed_data.get("experience_min_years", 0),
            parsed_data.get("experience_max_years", 0),
            parsed_data.get("employment_type"),
            parsed_data.get("workload_min"),
            parsed_data.get("workload_max"),
            parsed_data.get("remote_type"),
            parsed_data.get("management_responsibility", False),
            parsed_data.get("home_office_possible", False),
            parsed_data.get("education_level"),
            parsed_data.get("published_at")
        ))

        job_db_id = cursor.lastrowid

        # Required Skills
        for skill in parsed_data.get("required_skills", []):
            skill_id = get_or_create_skill(connection, skill)
            cursor.execute("""
                INSERT IGNORE INTO job_skills (job_id, skill_id, skill_type)
                VALUES (%s,%s,'required')
            """, (job_db_id, skill_id))

        # Preferred Skills
        for skill in parsed_data.get("preferred_skills", []):
            skill_id = get_or_create_skill(connection, skill)
            cursor.execute("""
                INSERT IGNORE INTO job_skills (job_id, skill_id, skill_type)
                VALUES (%s,%s,'preferred')
            """, (job_db_id, skill_id))

        connection.commit()
        print("✓ Job inserted successfully")
        return True

    except Error as e:
        print(f"✗ Error inserting job: {e}")
        return False


def get_all_job_links(connection) -> list[str]:
    """
    Fetch all job URLs (job_link) from the jobs table.
    
    Returns:
        list[str]: A list of job URLs. Empty list if no jobs found.
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT job_link FROM jobs")
        results = cursor.fetchall()
        # Extract job_link from each row (each row is a tuple)
        return [row[0] for row in results]
    except Error as e:
        print(f"✗ Error fetching job URLs: {e}")
        return []