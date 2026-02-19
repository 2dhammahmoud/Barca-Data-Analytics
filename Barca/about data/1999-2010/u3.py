import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
import os

# --- Configuration & Paths ---
folder_name = "BARCA_ERA"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

db_path = os.path.join(folder_name, 'barca_99_10.db')
excel_path = os.path.join(folder_name, 'barca_99_10_data.xlsx')
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
BARCA_TRANSFERS_99_10 = {
    # --- 1999-00 ---
    ("Dani García", "1999-00"): 15.00,
    ("Simão", "1999-00"): 14.00,
    ("Jari Litmanen", "1999-00"): 4.00,

    # --- 2000-01 ---
    ("Marc Overmars", "2000-01"): 29.30,
    ("Gerard López", "2000-01"): 21.60,
    ("Geovanni", "2000-01"): 20.00,
    ("Alfonso", "2000-01"): 16.50,
    ("Emmanuel Petit", "2000-01"): 15.00,

    # --- 2001-02 ---
    ("Javier Saviola", "2001-02"): 35.90,
    ("Philippe Christanval", "2001-02"): 17.00,
    ("Fábio Rochemback", "2001-02"): 9.00,
    ("Patrik Andersson", "2001-02"): 8.00,
    ("Roberto Bonano", "2001-02"): 4.10,
    ("Francesco Coco", "2001-02"): 3.50,

    # --- 2002-03 ---
    ("Juan Román Riquelme", "2002-03"): 11.00,
    ("Gaizka Mendieta", "2002-03"): 9.00,

    # --- 2003-04 ---
    ("Ronaldinho", "2003-04"): 32.25,
    ("Ricardo Quaresma", "2003-04"): 6.35,
    ("Rafa Márquez", "2003-04"): 5.25,

    # --- 2004-05 ---
    ("Samuel Eto'o", "2004-05"): 27.00,
    ("Deco", "2004-05"): 21.00,
    ("Ludovic Giuly", "2004-05"): 8.50,
    ("Edmílson", "2004-05"): 8.00,
    ("Maxi López", "2004-05"): 6.50,
    ("Juliano Belletti", "2004-05"): 6.00,
    ("Sylvinho", "2004-05"): 1.50,

    # --- 2006-07 ---
    ("Gianluca Zambrotta", "2006-07"): 14.00,
    ("Eidur Gudjohnsen", "2006-07"): 12.00,
    ("Lilian Thuram", "2006-07"): 5.00,

    # --- 2007-08 ---
    ("Thierry Henry", "2007-08"): 24.00,
    ("Gabriel Milito", "2007-08"): 20.00,
    ("Éric Abidal", "2007-08"): 15.00,
    ("Yaya Touré", "2007-08"): 9.00,

    # --- 2008-09 ---
    ("Dani Alves", "2008-09"): 35.50,
    ("Aleksandr Hleb", "2008-09"): 17.00,
    ("Martín Cáceres", "2008-09"): 16.50,
    ("Seydou Keita", "2008-09"): 14.00,
    ("Henrique", "2008-09"): 8.00,
    ("Gerard Piqué", "2008-09"): 5.00,

    # --- 2009-10 ---
    ("Zlatan Ibrahimović", "2009-10"): 69.50,
    ("Dmytro Chygrynskyi", "2009-10"): 25.00,
    ("Keirrison", "2009-10"): 14.00,
    ("Maxwell", "2009-10"): 5.00
}
COUNTRY_MAP = {
    'espanya': 'Spain', 'escocia': 'Scotland', 'inglaterra': 'England', 'gales': 'Wales',
    'olanda': 'Netherlands', 'holanda': 'Netherlands', 'alemanya': 'Germany','alemania': 'Germany',
    'turquia': 'Turkey','turquía': 'Turkey','italia': 'Italy','italía': 'Italy','suecia': 'Sweden',
    'mexico': 'Mexico','mèxic': 'Mexico','camerun': 'Cameroon','camerún': 'Cameroon','islandia': 'Iceland',
    'venezuela': 'Venezuela','suissa': 'Switzerland','suiza': 'Switzerland','austria': 'Austria',
    'costademarfil': 'Ivory Coast','costa de marfil': 'Ivory Coast',
    'uruguay': 'Uruguay','belgica': 'Belgium','bélgica': 'Belgium','mali': 'Mali','bielorrusia': 'Belarus',
    'ucrania': 'Ukraine','hongria': 'Hungary','hungria': 'Hungary','polonia': 'Poland',

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
        t_value = BARCA_TRANSFERS_99_10.get((p_name, season_label), "0")
        
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

for year in range(1999, 2010): # From 1999-00 to 2009-10
    next_year_short = str(year + 1)[2:]
    if year == 1999: next_year_short = "00"
    
    season_label = f"{year}-{next_year_short}"
    url = f"https://www.bdfutbol.com/en/t/t{season_label}1.html?t=lista"
    
    print(f"Scraping Season: {season_label}...")
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

print(f"\nDone! File saved at: {excel_path}")