import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
import os

# --- Configuration & Paths ---
folder_name = "BARCA_MODERN"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

db_path = os.path.join(folder_name, 'barca_2010_2025.db')
excel_path = os.path.join(folder_name, 'barca_2010_2025_data.xlsx')
BARCA_TRANSFERS_2010_2025 = {
    # --- 2010-11 ---
    ("David Villa", "2010-11"): 40.00,
    ("Javier Mascherano", "2010-11"): 20.00,
    ("Adriano", "2010-11"): 9.50,
    ("Ibrahim Afellay", "2010-11"): 3.00,

    # --- 2011-12 ---
    ("Cesc Fàbregas", "2011-12"): 34.00,
    ("Alexis Sánchez", "2011-12"): 26.00,

    # --- 2012-13 ---
    ("Alex Song", "2012-13"): 19.00,
    ("Jordi Alba", "2012-13"): 14.00,

    # --- 2013-14 ---
    ("Neymar", "2013-14"): 88.00,
    ("Bojan Krkic", "2013-14"): 13.00,

    # --- 2014-15 (الميركاتو التاريخي) ---
    ("Luis Suárez", "2014-15"): 81.70,
    ("Ivan Rakitic", "2014-15"): 18.00,
    ("Ter Stegen", "2014-15"): 12.00,
    ("Claudio Bravo", "2014-15"): 12.00,
    ("Jérémy Mathieu", "2014-15"): 20.00,
    ("Thomas Vermaelen", "2014-15"): 19.00,

    # --- 2015-16 ---
    ("Arda Turan", "2015-16"): 34.00,
    ("Aleix Vidal", "2015-16"): 17.00,

    # --- 2016-17 ---
    ("André Gomes", "2016-17"): 37.00,
    ("Paco Alcácer", "2016-17"): 30.00,
    ("Samuel Umtiti", "2016-17"): 25.00,
    ("Lucas Digne", "2016-17"): 16.50,
    ("Jasper Cillessen", "2016-17"): 13.00,

    # --- 2017-18 ---
    ("Philippe Coutinho", "2017-18"): 135.00,
    ("Ousmane Dembélé", "2017-18"): 135.00,
    ("Paulinho", "2017-18"): 40.00,
    ("Nélson Semedo", "2017-18"): 35.70,
    ("Arturo Vidal", "2018-19"): 18.00,
    ("Yerry Mina", "2017-18"): 12.40,
    ("Deulofeu", "2017-18"): 12.00,
    ("Marlon", "2017-18"): 5.00,

    # --- 2018-19 ---
    ("Malcom", "2018-19"): 41.00,
    ("Clément Lenglet", "2018-19"): 35.90,
    ("Arthur Melo", "2018-19"): 31.00,
    ("Arturo Vidal", "2018-19"): 18.00,
    ("Emerson Royal", "2018-19"): 12.00,
    ("Jeison Murillo", "2018-19"): 1.20,
    ("Jean-Clair Todibo", "2018-19"): 1.00,
    ("Kevin-Prince Boateng", "2018-19"): 1.00,

    # --- 2019-20 ---
    ("Antoine Griezmann", "2019-20"): 120.00,
    ("Frenkie de Jong", "2019-20"): 86.00,
    ("Neto", "2019-20"): 26.00,
    ("Pedri", "2019-20"): 23.00,
    ("Junior Firpo", "2019-20"): 20.00,
    ("Martin Braithwaite", "2019-20"): 18.00,
    ("Matheus Fernandes", "2019-20"): 7.00,
    ("Marc Cucurella", "2019-20"): 4.00,

    # --- 2020-21 ---
    ("Miralem Pjanic", "2020-21"): 60.00,
    ("Sergiño Dest", "2020-21"): 21.00,
    ("Trincão", "2020-21"): 30.94,
    

    # --- 2021-22 ---
    ("Ferran Torres", "2021-22"): 55.00,
    ("Emerson Royal", "2021-22"): 14.00,
    ("Yusuf Demir", "2021-22"): 0.50,

    # --- 2022-23 ---
    ("Raphinha", "2022-23"): 58.00,
    ("Jules Koundé", "2022-23"): 50.00,
    ("Robert Lewandowski", "2022-23"): 45.00,
    ("Pablo Torre", "2022-23"): 6.00,

    # --- 2023-24 ---
    ("Oriol Romeu", "2023-24"): 3.40,
    ("Vitor Roque", "2023-24"): 30.00,
    


    # --- 2024-25 ---
    ("Dani Olmo", "2024-25"): 55.00,
    ("Pau Víctor", "2024-25"): 5.50,

    #--- 2025-26 ---
    
    ("Joan García", "2025-26"): 25.00,
    ("Roony Bardghji", "2025-26"): 2.50,

}

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
    'olanda': 'Netherlands', 'holanda': 'Netherlands', 'alemanya': 'Germany','alemania': 'Germany',
    'turquia': 'Turkey','turquía': 'Turkey','italia': 'Italy','italía': 'Italy','suecia': 'Sweden',
    'mexico': 'Mexico','mèxic': 'Mexico','camerun': 'Cameroon','camerún': 'Cameroon','islandia': 'Iceland',
    'venezuela': 'Venezuela','suissa': 'Switzerland','suiza': 'Switzerland','austria': 'Austria',
    'costademarfil': 'Ivory Coast','costa de marfil': 'Ivory Coast',
    'uruguay': 'Uruguay','belgica': 'Belgium','bélgica': 'Belgium','mali': 'Mali','bielorrusia': 'Belarus',
    'ucrania': 'Ukraine','hongria': 'Hungary','hungria': 'Hungary','polonia': 'Poland','estadosunidos': 'USA',
    'estatsunits': 'USA','republicadominicana': 'Dominican Republic','marruecos': 'Morocco',

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

def init_database():
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
        t_value = BARCA_TRANSFERS_2010_2025.get((p_name, season_label), "0")
        
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

for year in range(2010, 2026): # From 2010-11 to 2025-26
    next_year_short = str(year + 1)[2:]
    season_label = f"{year}-{next_year_short}"
    url = f"https://www.bdfutbol.com/en/t/t{season_label}1.html?t=lista"
    
    print(f"Processing Season: {season_label}...")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            m_table = soup.find('table', {'id': 'taulaentrenadors'})
            manager = m_table.find_all('tr')[-1].find_all('td')[2].text.strip() if m_table else "Unknown"
            
            data = scrape_season_data(response.content, season_label, manager)
            db_conn.cursor().executemany("INSERT INTO players_stats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
            db_conn.commit()
            print(f"Successfully processed {len(data)} players.")
        
        time.sleep(2)
            
    except Exception as e:
        print(f"Error in {season_label}: {e}")

db_conn.close()

# --- Export to Excel ---
final_conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM players_stats", final_conn)
df.to_excel(excel_path, index=False)
final_conn.close()

print(f"\nSuccess! Data exported to: {excel_path}")