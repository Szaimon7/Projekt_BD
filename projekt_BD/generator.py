import sqlite3
import random
from mimesis import Generic
from mimesis.locales import Locale
from datetime import datetime, timedelta
import pandas as pd

# !!!SKONCZONE ZALOZENIA 1 I 2 
DB_NAME = "Projekt_bazy.db"
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 1, 1)
MIN_WAGE = 4806  
NUM_GUESTS = 600
NUM_EMPLOYEES = 15

#zmiana biblioteki na mimesis, działa szybciej i lepiej wsipera PL
fake = Generic(locale = Locale.PL)

def create_schema(cursor):
    # usuniecie starych tabelek
    cursor.executescript("""
        DROP TABLE IF EXISTS Incydenty;
        DROP TABLE IF EXISTS Aktywnosci_Atrakcje;
        DROP TABLE IF EXISTS Transakcje_Ubezpieczenia;
        DROP TABLE IF EXISTS Przeglady_Techniczne;
        DROP TABLE IF EXISTS Wizyty;
        DROP TABLE IF EXISTS Ubezpieczenia_Typy;
        DROP TABLE IF EXISTS Bilety_Cennik;
        DROP TABLE IF EXISTS Atrakcje;
        DROP TABLE IF EXISTS Klienci;
        DROP TABLE IF EXISTS Pracownicy;
    """)

    cursor.executescript("""
        CREATE TABLE Pracownicy (
            id INTEGER PRIMARY KEY,
            imie TEXT, nazwisko TEXT, stanowisko TEXT, pensja REAL, data_zatrudnienia DATE
        );
        CREATE TABLE Klienci (
            id INTEGER PRIMARY KEY,
            imie TEXT, nazwisko TEXT, data_urodzenia DATE, plec TEXT
        );
        CREATE TABLE Atrakcje (
            id INTEGER PRIMARY KEY,
            nazwa TEXT, typ TEXT, czy_vr_dostepne BOOLEAN, koszt_utrzymania_h REAL, data_zakupu DATE
        );
        CREATE TABLE Bilety_Cennik (
            id INTEGER PRIMARY KEY,
            nazwa TEXT, cena REAL
        );
        CREATE TABLE Ubezpieczenia_Typy (
            id INTEGER PRIMARY KEY,
            nazwa TEXT, cena REAL, max_wyplata REAL
        );
        CREATE TABLE Wizyty (
            id INTEGER PRIMARY KEY,
            klient_id INTEGER, data_wizyty DATE, bilet_id INTEGER,
            FOREIGN KEY(klient_id) REFERENCES Klienci(id),
            FOREIGN KEY(bilet_id) REFERENCES Bilety_Cennik(id)
        );
        CREATE TABLE Transakcje_Ubezpieczenia (
            id INTEGER PRIMARY KEY,
            wizyta_id INTEGER, ubezpieczenie_id INTEGER,
            FOREIGN KEY(wizyta_id) REFERENCES Wizyty(id),
            FOREIGN KEY(ubezpieczenie_id) REFERENCES Ubezpieczenia_Typy(id)
        );
        CREATE TABLE Aktywnosci_Atrakcje (
            id INTEGER PRIMARY KEY,
            wizyta_id INTEGER, atrakcja_id INTEGER, czas_rozpoczecia DATETIME, tryb_vr BOOLEAN,
            FOREIGN KEY(wizyta_id) REFERENCES Wizyty(id),
            FOREIGN KEY(atrakcja_id) REFERENCES Atrakcje(id)
        );
        CREATE TABLE Incydenty (
            id INTEGER PRIMARY KEY,
            aktywnosc_id INTEGER, opis TEXT, wyplacone_odszkodowanie REAL,
            FOREIGN KEY(aktywnosc_id) REFERENCES Aktywnosci_Atrakcje(id)
        );
        CREATE TABLE Przeglady_Techniczne (
            id INTEGER PRIMARY KEY,
            atrakcja_id INTEGER, data_przegladu DATE, koszt REAL, czy_awaria BOOLEAN, opis TEXT,
            FOREIGN KEY(atrakcja_id) REFERENCES Atrakcje(id)
        );
    """)
    print("Schemat bazy utworzony pomyślnie.")


#brak zmian, atrakcje spoko :)
def populate_data(cursor):
    # 1. atrakcje 
    atrakcje_lista = [
        ("Hyperion", "Rollercoaster", False, 150.0),
        ("Symulator kopania kryptowalut", "Edukacja", True, 20.0),
        ("Wróżenie z fusów", "Usługa", True, 10.0),
        ("AI Horror House", "Dom Strachów", True, 80.0),
        ("Nocny wyscig", "Dark Ride", True, 50.0),
        ("dziecieca karuzela", "Karuzela", False, 40.0),
        ("Blockchain Bungee", "Ekstremalne", False, 20.0),
        ("Multiwersum", "Huśtawka", True, 15.0),
        ("Maszt 5G", "Wieża spadania", False, 100.0),
        ("Cyber Truck", "Autodrom", False, 60.0)
    ]
    
    #data zakupu - losowa data między 5 a 2 lata temu spójnie ze schematem
    for a in atrakcje_lista:
        years_ago = random.randint(2, 5)
        months_ago = random.randint(0, 11) #miesiące przesunięte indesksami
        days_ago = random.randint(0, 28) #tak samo jak dla miesięcy
        data_zakupu = datetime.now() - timedelta(days=365*years_ago + 30*months_ago + days_ago) #liczy datę odejmując czas z zakresu 2-5 lat
    
        #dodanie do bazt
        cursor.execute("INSERT INTO Atrakcje (nazwa, typ, czy_vr_dostepne, koszt_utrzymania_h, data_zakupu) VALUES (?,?,?,?,?)",
        (a[0], a[1], a[2], a[3], data_zakupu.date()))    
    
    #cenniki i ubezpieczenia 
    bilety = [("Standard", 100), ("VIP", 250), ("Student", 60), ("VR", 80)]
    cursor.executemany("INSERT INTO Bilety_Cennik (nazwa, cena) VALUES (?,?)", bilety)

    #typy ubezpieczen
    ubezpieczenia = [
        ("Utrata mienia do 500 zł", 15.0, 500.0),
        ("Ubezpieczenie od wypadku", 30.0, 5000.0),
        ("Ubezpieczenie zdrowotne VR", 20.0, 2000.0),
        ("Ubezpieczenie od kradzieży", 10.0, 1000.0),
        ("Ubezpieczenie od stresu", 25.0, 3000.0)
    ]
    
    #dodanie do bazy
    cursor.executemany("INSERT INTO Ubezpieczenia_Typy (nazwa, cena, max_wyplata) VALUES (?,?,?)", ubezpieczenia)

    #pracownicy
    stanowisko = [
        ("Kierownik atrakcji", 6000, 8000),
        ("Operator", 4806, 5500),
        ("Kasjer", 4806, 4900),
        ("Technik", 5000, 7000),
        ("Ochroniarz", 4806, 5000),
        ("Pracownik serwisowy", 4806, 4900),
        ("Instruktor", 4806, 6000)
    ]
    
    for i in range(NUM_EMPLOYEES):
        stanowiska = random.choice(stanowiska)
        pensja = round(random.uniform(stanowisko[1], stanowisko[2]), 2) #generowanie pensji
        
        #generowanie daty zatrudnienia
        months_ago = random.randint(12, 24) #minimum rok pracy w firmie
        days_ago = random.randint(0, 30)
        data_zatrudnienia = datetime.now() - timedelta(days=30*months_ago + days_ago) #obliczanie daty od obecnego czasu minus czas pracy
        
        #dodanie do bazy
        cursor.execute("INSERT INTO Pracownicy (imie, nazwisko, stanowisko, pensja, data_zatrudnienia) VALUES (?,?,?,?,?)",
        (fake.person.first_name(), fake.person.last_name(), stanowisko[0], pensja, data_zatrudnienia.date()))
        

    #klienci
    for i in range(NUM_GUESTS):
        #generowanie daty ur.
        wiek = random.randint(10, 70)
        birth_year = datetime.now().year - wiek
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  #unikanie problemów z lutym
        data_urodzenia = datetime(birth_year, birth_month, birth_day)
        
        #dodwanie do bazy
        cursor.execute("INSERT INTO Klienci (imie, nazwisko, data_urodzenia, plec) VALUES (?,?,?,?)",
        (fake.person.first_name(), fake.person.last_name(), data_urodzenia.date(), random.choice(['M', 'K'])))

    #generowanie ruchu (wizyty, aktywności, wypadki)
    #rok wizyt 2024
    current_date = START_DATE
    total_visits = 0
    
    while current_date <= END_DATE:
        # losowa liczba gości danego dnia (z przewaga w weekendy) - realistyczne wartości
        is_weekend = current_date.weekday() >= 5
        if current_date.month in [6, 7, 8]:  #lato
            daily_guests = random.randint(20, 50) if not is_weekend else random.randint(40, 80)
        elif current_date.month in [12]:  #grudzień
            daily_guests = random.randint(30, 60) if not is_weekend else random.randint(50, 90)
        else:  #pozostałe miesiące
            daily_guests = random.randint(5, 15) if not is_weekend else random.randint(20, 40)
        
        # pobierz ID klientów biletów i ubezpieczeń
        klient_ids = [row[0] for row in cursor.execute("SELECT id FROM Klienci").fetchall()]
        bilet_ids = [row[0] for row in cursor.execute("SELECT id FROM Bilety_Cennik").fetchall()]
        ubezp_ids = [row[0] for row in cursor.execute("SELECT id FROM Ubezpieczenia_Typy").fetchall()]
        atrakcje = cursor.execute("SELECT id, czy_vr_dostepne FROM Atrakcje").fetchall()

        # symulacja dnia
        if len(klient_ids) >= daily_guests:
            todays_visitors = random.sample(klient_ids, daily_guests)
        else:
            todays_visitors = klient_ids
        
        # dla każdego gościa danego dnia
        for k_id in todays_visitors:
            total_visits += 1
           
            # tworzenie wizyty
            bilet = random.choice(bilet_ids)
            cursor.execute("INSERT INTO Wizyty (klient_id, data_wizyty, bilet_id) VALUES (?,?,?)", 
                          (k_id, current_date.date(), bilet))
            wizyta_id = cursor.lastrowid
            
            # kupno ubezpieczeń (szansa 50%) - realistyczne
            if random.random() > 0.5:
                # 1-2 ubezpieczenia na wizytę (realistyczne)
                num_insurances = random.randint(1, 2)
                wybrane_ubezp = random.sample(ubezp_ids, k=min(num_insurances, len(ubezp_ids)))
                for u_id in wybrane_ubezp:
                    cursor.execute("INSERT INTO Transakcje_Ubezpieczenia (wizyta_id, ubezpieczenie_id) VALUES (?,?)", 
                                  (wizyta_id, u_id))
            
            # używanie atrakcji (3-8 jak w oryginale)
            liczba_atrakcji = random.randint(3, 8)
            for _ in range(liczba_atrakcji):
                atr = random.choice(atrakcje)
                tryb_vr = True if (atr[1] and random.random() > 0.5) else False
                
                # Realistyczna godzina w ciągu dnia
                godzina = random.randint(10, 19)
                minuta = random.randint(0, 59)
                czas_rozpoczecia = datetime(current_date.year, current_date.month, current_date.day, godzina, minuta)
                
                cursor.execute("INSERT INTO Aktywnosci_Atrakcje (wizyta_id, atrakcja_id, czas_rozpoczecia, tryb_vr) VALUES (?,?,?,?)",
                               (wizyta_id, atr[0], czas_rozpoczecia, tryb_vr))
                akt_id = cursor.lastrowid
                
                # wypadek (szansa 1%) - realistyczna częstość
                if random.random() < 0.01:
                    # Realistyczne odszkodowania
                    odszkodowanie = round(random.uniform(50, 1000), 2)
                    opis = random.choice([
                        "Zgubiona czapka na rollercoasterze",
                        "Kanapka porwana przez gołębie", 
                        "Zawroty głowy w VR",
                        "Uszkodzenie okularów VR",
                        "Plama na ubraniu"
                    ])
                    cursor.execute("INSERT INTO Incydenty (aktywnosc_id, opis, wyplacone_odszkodowanie) VALUES (?,?,?)",
                                   (akt_id, opis, odszkodowanie))

        # przeglady techniczne i awarie - realistyczne koszty
        if random.random() < 0.1 and atrakcje:
            atr = random.choice(atrakcje)
            # Realistyczne koszty przeglądów
            koszt = round(random.uniform(500, 5000), 2)
            awaria = random.choice([True, False])
            opis_serwisu = random.choice([
                "Przegląd okresowy", "Wymiana łożysk", "Kalibracja czujników", 
                "Aktualizacja oprogramowania", "Czyszczenie mechanizmów"
            ]) if not awaria else random.choice([
                "Awaria silnika", "Uszkodzenie VR", "Błąd systemu", "Pęknięcie elementu"
            ])
            cursor.execute("INSERT INTO Przeglady_Techniczne (atrakcja_id, data_przegladu, koszt, czy_awaria, opis) VALUES (?,?,?,?,?)",
                           (atr[0], current_date.date(), koszt, awaria, opis_serwisu))

        current_date += timedelta(days=1)
    
    print(f"Wygenerowano {total_visits} wizyt (wymagane: minimum 500)")
    print("Baza wypełniona danymi z poprawionymi wartościami")

# Uruchomienie
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
create_schema(cursor)
populate_data(cursor)
conn.commit()
conn.close()