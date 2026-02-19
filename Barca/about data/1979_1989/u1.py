import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time

# 1. Mapping Configuration
POSITION_MAP = {
    'por': 'Goalkeeper',
    'def': 'Defender',
    'ltd': 'Right Back',
    'lti': 'Left Back',
    'cen': 'Center Back',
    'mig': 'Midfielder',
    'dav': 'Forward',
    'dac': 'Center Forward'
}
COUNTRY_MAP = {
    'espanya': 'Spain',
    'escocia': 'Scotland',
    'inglaterra': 'England',
    'olanda': 'Netherlands',
    'alemanya': 'Germany',
    'alemania': 'Germany',
    'franca': 'France',
    'brasil': 'Brazil',
    'argentina': 'Argentina',
    'paraguay': 'Paraguay',
    'austria': 'Austria',
    'dinamarca': 'Denmark',
    'gales': 'Wales',
    'uruguay': 'Uruguay',
    'belgica': 'Belgium'
}

# Updated Transfer Fees Dictionary (Based on your latest data)
TRANSFER_FEES = {
    # --- 1979-80 ---
    ('Simonsen', '1979-80'): '660k', ('Roberto Dinamite', '1979-80'): '300k', 
    ('Canito', '1979-80'): '270k', ('Landaburu', '1979-80'): '180k', ('Amigó', '1979-80'): '72k',
    
    # --- 1980-81 ---
    ('Bernhard Schuster', '1980-81'): '1.14M', ('Alexanko', '1980-81'): '720k', 
    ('Quini', '1980-81'): '480k', ('Amador', '1980-81'): '150k',
    
    # --- 1981-82 ---
    ('Víctor Muñoz', '1981-82'): '510k', ('Urruti', '1981-82'): '390k', 
    ('Morán', '1981-82'): '390k', ('Gerardo', '1981-82'): '270k', ('Cleo', '1981-82'): '90k',
    
    # --- 1982-83 ---
    ('Maradona', '1982-83'): '7.30M', ('Marcos Alonso', '1982-83'): '720k', 
    ('Julio Alberto', '1982-83'): '480k', ('Periko Alonso', '1982-83'): '420k', 
    ('Urbano', '1982-83'): '420k', ('Pichi Alonso', '1982-83'): '420k',
    
    # --- 1983-84 ---
    ('Gabrich', '1983-84'): '120k',
    
    # --- 1984-85 ---
    ('Archibald', '1984-85'): '2.00M',
    
    # --- 1985-86 ---
    ('Amarilla', '1985-86'): '300k',
    
    # --- 1986-87 ---
    ('Lineker', '1986-87'): '3.20M', ('Hughes', '1986-87'): '3.00M', 
    ('Zubizarreta', '1986-87'): '1.90M', ('Robert', '1986-87'): '750k',
    
    # --- 1988-89 ---
    ('López Rekarte', '1988-89'): '960k', ('Serna', '1988-89'): '1.25M', 
    ('Aloísio', '1988-89'): '1.20M', ('Eusebio', '1988-89'): '1.10M', 
    ('Bakero', '1988-89'): '1.38M', ('Begiristain', '1988-89'): '1.38M', 
    ('Julio Salinas', '1988-89'): '1.10M', ('Unzué', '1988-89'): '1.10M', 
    ('Soler', '1988-89'): '1.50M', ('Valverde', '1988-89'): '1.50M', 
    ('Manolo Hierro', '1988-89'): '900k', ('Romerito', '1988-89'): '240k'
}

# 2. Database Initialization
def init_database():
    """Initializes the SQLite database with the updated nationality column."""
    connection = sqlite3.connect('barca_80s.db')
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS players_stats (
            player_name TEXT,
            season TEXT,
            nationality TEXT,
            position TEXT,
            age INTEGER,
            matches_played INTEGER,
            matches_started INTEGER,
            matches_completed INTEGER,
            matches_as_substitute INTEGER,
            total_cards INTEGER,
            minutes_played INTEGER,
            yellow_cards INTEGER,
            red_cards INTEGER,
            goals INTEGER,
            goal_contributions INTEGER,
            manager_name TEXT,
            transfer_value TEXT
        )
    ''')
    connection.commit()
    return connection

# 3. Data Extraction Logic
def scrape_season_data(html_content, season_label, manager_name):
    """Parses HTML and extracts comprehensive player statistics."""
    soup = BeautifulSoup(html_content, 'html.parser')
    players_list = []
    
    table = soup.find('table', {'id': 'c3p0'})
    if not table:
        return []
        
    rows = table.find_all('tr')[1:]  # Skip the header row
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 15: 
            continue
        
        # Player Name
        p_name = cols[3].text.strip()
        
# Nationality Extraction
        nat_div = cols[2].find('div', class_='pais')
        nationality = "Unknown"
        
        if nat_div:
            # بنسحب كل الكلاسات ونحولها لحروف صغيرة ونشيل أي مسافات زيادة
            classes = [c.strip().lower() for c in nat_div.get('class', [])]
            
            # بندور على أي كلاس من اللي سحبناهم موجود في القاموس بتاعنا
            found = False
            for c in classes:
                if c in COUNTRY_MAP:
                    nationality = COUNTRY_MAP[c]
                    found = True
                    break
            
            # لو ملقتش أي حاجة في القاموس، خد الكلاس التاني (اللي هو اسم الدولة)
            if not found and len(classes) > 1:
                nationality = classes[1].capitalize()

        # Position Extraction
        pos_div = cols[4].find('div')
        pos_class = pos_div.get('class')[0] if pos_div else 'unknown'
        pos_text = POSITION_MAP.get(pos_class, 'Other')
        
        # Stats & Values
        goals_val = int(cols[14].text) if cols[14].text.isdigit() else 0
        t_value = TRANSFER_FEES.get((p_name, season_label), "0")
        
        player_data = (
            p_name,
            season_label,
            nationality,
            pos_text,
            int(cols[5].text) if cols[5].text.isdigit() else None,
            int(cols[5].text) if cols[5].text.isdigit() else 0,
            int(cols[6].text) if cols[6].text.isdigit() else 0,
            int(cols[7].text) if cols[7].text.isdigit() else 0,
            int(cols[8].text) if cols[8].text.isdigit() else 0,
            (int(cols[12].text) if cols[12].text.isdigit() else 0) + (int(cols[13].text) if cols[13].text.isdigit() else 0),
            int(cols[11].text) if cols[11].text.isdigit() else 0,
            int(cols[12].text) if cols[12].text.isdigit() else 0,
            int(cols[13].text) if cols[13].text.isdigit() else 0,
            goals_val,
            goals_val, # goal_contributions
            manager_name,
            t_value
        )
        players_list.append(player_data)
        
    return players_list

# 4. Save to DB Function
def save_to_db(connection, data):
    cursor = connection.cursor()
    cursor.executemany('''
        INSERT INTO players_stats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', data)
    connection.commit()

# --- Execution ---
db_conn = init_database()
print("Database initialized with 'nationality' column. Ready for extraction.")


# Main Loop for the 1980s Decade (1979-1989)
conn = init_database()

for year in range(1979, 1989):
    next_year_short = str(year + 1)[2:]
    season_label = f"{year}-{next_year_short}"
    
    # URL with the list parameter you provided
    url = f"https://www.bdfutbol.com/en/t/t{year}-{next_year_short}1.html?t=lista"
    
    print(f"--- Processing Season: {season_label} ---")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 1. Automatic Manager Extraction
            m_table = soup.find('table', {'id': 'taulaentrenadors'})
            current_manager = m_table.find_all('tr')[-1].find_all('td')[2].text.strip() if m_table else "Unknown"
            
            # 2. Scrape Player Data using the manager we just found
            season_data = scrape_season_data(response.content, season_label, current_manager)
            
            if season_data:
                save_to_db(conn, season_data)
                print(f"Saved {len(season_data)} players. Manager: {current_manager}")
            
        time.sleep(2) # Delay to stay safe
            
    except Exception as e:
        print(f"Error in {season_label}: {e}")

conn.close()
print("\nExtraction for the 80s is complete!")





import sqlite3
import pandas as pd

# 1. Connect to your SQLite database
conn = sqlite3.connect('barca_80s.db')

# 2. Read the table into a DataFrame
df = pd.read_sql_query("SELECT * FROM players_stats", conn)

# 3. Export the DataFrame to an Excel file
# Note: You might need to install 'openpyxl' via (pip install openpyxl)
df.to_excel('barca_80s_data.xlsx', index=False)

conn.close()
print("Success! Your data has been saved to 'barca_80s_data.xlsx'.")