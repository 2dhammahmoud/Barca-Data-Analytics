
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
import os

# --- Configuration & Paths ---
folder_name = "BARCA"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

db_path = os.path.join(folder_name, 'barca_90s.db')
excel_path = os.path.join(folder_name, 'barca_90s_data.xlsx')

# --- Data Mapping Dictionaries ---
POSITION_MAP = {
    'por': 'Goalkeeper',
    'def': 'Defender',
    'mig': 'Midfielder',
    'dav': 'Forward',
    'cen': 'Center Back',
    'ltd': 'Right Back',
    'lti': 'Left Back',
    'dac': 'Center Forward'
}
COUNTRY_MAP = {
    'espanya': 'Spain', 'escocia': 'Scotland', 'inglaterra': 'England', 'gales': 'Wales',
    'olanda': 'Netherlands', 'holanda': 'Netherlands', 'alemanya': 'Germany',
    'brasil': 'Brazil', 'argentina': 'Argentina', 'portugal': 'Portugal',
    'franca': 'France', 'francia': 'France', 'bulgaria': 'Bulgaria',
    'romania': 'Romania', 'nigeria': 'Nigeria', 'croacia': 'Croatia', 'serbia': 'Serbia',
    'dinamarca': 'Denmark',
    'rumania': 'Romania',
    'romania': 'Romania', 
    'rússia': 'Russia',     
    'rusia': 'Russia',    
    'bòsnia': 'Bosnia',
    'bosnia': 'Bosnia',
    'finlàndia': 'Finland',
    'finlandia': 'Finland'
}

# Add the transfer fees dictionary we prepared earlier here
TRANSFER_FEES_90s = {
    # --- 1989-90 (وصول كومان) ---
    ('Koeman', '1989-90'): '5.60M', ('Laudrup', '1989-90'): '2.00M',
    
    # --- 1990-91 (ستويتشكوف) ---
    ('Stoichkov', '1990-91'): '2.10M', ('Nando', '1990-91'): '600k',
    ('Goikoetxea', '1990-91'): '1.50M', ('Ferrer', '1990-91'): '0', # ناشئين
    
    # --- 1991-92 ---
    ('Juan Carlos', '1991-92'): '1.30M', ('Witschge', '1991-92'): '2.10M',
    ('Nadal', '1991-92'): '1.20M',
    
    # --- 1992-93 ---
    ('Vivas', '1992-93'): '0', # انتقال حر
    
    # --- 1993-94 (روماريو) ---
    ('Romário', '1993-94'): '2.70M', ('Sergi Barjuán', '1993-94'): '0', # ناشئين
    
    # --- 1994-95 ---
    ('Hagi', '1994-95'): '3.00M', ('Abelardo', '1994-95'): '1.65M',
    ('Eskurza', '1994-95'): '1.50M', ('Lopetegui', '1994-95'): '360k',
    
    # --- 1995-96 (بداية النهاية لكرويف) ---
    ('Figo', '1995-96'): '2.25M', ('Kodro', '1995-96'): '4.20M',
    ('Prosinecki', '1995-96'): '0', ('Popescu', '1995-96'): '2.40M',
    
    # --- 1996-97 (رونالدو الظاهرة - موسم بوبي روبسون) ---
    ('Ronaldo', '1996-97'): '15.00M', ('Giovanni', '1996-97'): '4.50M',
    ('Luis Enrique', '1996-97'): '0', ('Vítor Baía', '1996-97'): '3.90M',
    ('Pizzi', '1996-97'): '2.10M', ('Laurent Blanc', '1996-97'): '0',
    ('Couto', '1996-97'): '1.80M',
    
    # --- 1997-98 (ريفالدو - حقبة فان غال) ---
    ('Rivaldo', '1997-98'): '23.50M', ('Sonny Anderson', '1997-98'): '18.00M',
    ('Dugarry', '1997-98'): '4.00M', ('Hesp', '1997-98'): '1.00M',
    ('Reiziger', '1997-98'): '4.80M', ('Dragan Ciric', '1997-98'): '1.80M',
    
    # --- 1998-99 ---
    ('Kluivert', '1998-99'): '12.00M', ('Cocu', '1998-99'): '0',
    ('Zenden', '1998-99'): '7.20M', ('Frank de Boer', '1998-99'): '7.50M',
    ('Ronald de Boer', '1998-99'): '7.50M',
    
    # --- 1999-00 ---
    ('Litmanen', '1999-00'): '0', ('Dani', '1999-00'): '12.60M',
    ('Simão', '1999-00'): '15.00M', ('Bogarde', '1999-00'): '0'
}
def init_database():
    """Initializes the SQLite database with the required schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS players_stats (
            player_name TEXT, season TEXT, nationality TEXT, position TEXT,
            age INTEGER, matches_played INTEGER, matches_started INTEGER,
            matches_completed INTEGER, matches_as_substitute INTEGER,
            total_cards INTEGER, minutes_played INTEGER, yellow_cards INTEGER,
            red_cards INTEGER, goals INTEGER, goal_contributions INTEGER,
            manager_name TEXT, transfer_value TEXT
        )
    ''')
    conn.commit()
    return conn
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
            classes = [c.strip().lower() for c in nat_div.get('class', [])]
            
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
        t_value = TRANSFER_FEES_90s.get((p_name, season_label), "0")
        
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
# --- Main Execution Loop ---
db_conn = init_database()

for year in range(1989, 2000): # Iterating from 89-90 to 99-00
    next_year_short = str(year + 1)[2:]
    if year == 1999: 
        next_year_short = "00"
    
    season_label = f"{year}-{next_year_short}"
    url = f"https://www.bdfutbol.com/en/t/t{season_label}1.html?t=lista"
    
    print(f"Scraping Season: {season_label}...")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extracting Manager from the specific manager table
            m_table = soup.find('table', {'id': 'taulaentrenadors'})
            manager = m_table.find_all('tr')[-1].find_all('td')[2].text.strip() if m_table else "Unknown"
            
            # Extract and Save Player Data
            data = scrape_season_data(response.content, season_label, manager)
            db_conn.cursor().executemany("INSERT INTO players_stats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
            db_conn.commit()
            print(f"Successfully processed {len(data)} players.")
        
        # Sleep to avoid server-side rate limiting
        time.sleep(2)
            
    except Exception as e:
        print(f"Error occurred in season {season_label}: {e}")

db_conn.close()

# --- Exporting to Excel for Analysis ---
final_conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM players_stats", final_conn)
df.to_excel(excel_path, index=False)
final_conn.close()

print(f"\nAll data successfully exported to: {excel_path}")
