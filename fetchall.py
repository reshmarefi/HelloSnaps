import psycopg2


# Define your connection parameters
db_params = {
    'database': 'hellosnaps',
    'user': 'doadmin',
    'password': 'AVNS_Zb9uTNTgcOU1HHjjuft',
    'host': 'hellosnaps-db-do-user-12169058-0.c.db.ondigitalocean.com',
    'port': '25060'  # default PostgreSQL port is 5432
}

Workspace = '9mJcHfTBLFlrisV285S44'
query = 'SELECT * FROM public."Media";'

# Prefix for the image URLs
url_prefix = "https://blr1.digitaloceanspaces.com/snap-bucket/"

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    print("Contents of the Media table:")
    for row in rows:
        print(row) 
    print("Image URLs with prefix:")
    for row in rows:
        # Concatenate the prefix with the image link
        full_image_url = url_prefix + row[2]
        print(full_image_url)

except psycopg2.DatabaseError as error:
    print(f"Error: {error}")

finally:
    if conn is not None:
        conn.close()
        
        
