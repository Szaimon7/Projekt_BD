import sqlite3
import random
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd

# !!!SKONCZONE ZALOZENIA 1 I 2 DO "Rozmiar bazy powinien być rozsądny (porównywalny z tym, co może potrzebować rzeczywista firma prowadząca park rozrywki). Czas działania firmy to przynajmniej rok, zatrudnia obecnie minimum 5 pracowników, wypłacając im adekwatne i zgodne z prawem wynagrodzenie. W poprzednim roku działalności przyjęto przynajmniej 500 gości, którzy bawili się na przynajmniej 10 atrakcjach."
# Konfiguracja
DB_NAME = "Projekt_bazy.db"
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 1, 1)
MIN_WAGE = 4300  
NUM_GUESTS = 600
NUM_EMPLOYEES = 15

fake = Faker('pl_PL')

def create_schema(cursor):
    # Usunięcie starych tabel jeśli istnieją
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

def populate_data(cursor):
    # 1. Atrakcje 
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
    for a in atrakcje_lista:
        cursor.execute("INSERT INTO Atrakcje (nazwa, typ, czy_vr_dostepne, koszt_utrzymania_h, data_zakupu) VALUES (?,?,?,?,?)",
                       (a[0], a[1], a[2], a[3], fake.date_between(start_date='-5y', end_date='-2y')))

    # 2. Cenniki i Ubezpieczenia 
    bilety = [("Standard", 100), ("VIP", 250), ("Student", 60), ("VR", 80)]
    cursor.executemany("INSERT INTO Bilety_Cennik (nazwa, cena) VALUES (?,?)", bilety)

    ubezpieczenia = [
        ("Utrata czapki", 15, 200),
        ("Atak gołębia", 5, 50),
        ("Mdłości VR", 10, 100),
        ("Egzystencjalny lęk", 50, 1000)
    ]
    cursor.executemany("INSERT INTO Ubezpieczenia_Typy (nazwa, cena, max_wyplata) VALUES (?,?,?)", ubezpieczenia)

    # 3. Pracownicy
    roles = ["Operator", "Kasjer", "Technik", "Ochrona", "Aktor VR"]
    for _ in range(NUM_EMPLOYEES):
        pensja = round(random.uniform(MIN_WAGE, MIN_WAGE * 2.5), 2)
        cursor.execute("INSERT INTO Pracownicy (imie, nazwisko, stanowisko, pensja, data_zatrudnienia) VALUES (?,?,?,?,?)",
                       (fake.first_name(), fake.last_name(), random.choice(roles), pensja, fake.date_between(start_date='-2y', end_date='-1y')))

    # 4. Klienci
    for _ in range(NUM_GUESTS):
        cursor.execute("INSERT INTO Klienci (imie, nazwisko, data_urodzenia, plec) VALUES (?,?,?,?)",
                       (fake.first_name(), fake.last_name(), fake.date_of_birth(minimum_age=10, maximum_age=70), random.choice(['M', 'K'])))

    # 5. Generowanie ruchu (Wizyty, Aktywności, Incydenty)
    current_date = START_DATE
    while current_date <= END_DATE:
        # Losowa liczba gości danego dnia (więcej w weekendy)
        is_weekend = current_date.weekday() >= 5
        daily_guests = random.randint(5, 15) if not is_weekend else random.randint(20, 50)
        
        # Pobierz ID klientów, biletów, ubezpieczeń
        klient_ids = [row[0] for row in cursor.execute("SELECT id FROM Klienci").fetchall()]
        bilet_ids = [row[0] for row in cursor.execute("SELECT id FROM Bilety_Cennik").fetchall()]
        ubezp_ids = [row[0] for row in cursor.execute("SELECT id FROM Ubezpieczenia_Typy").fetchall()]
        atrakcje = cursor.execute("SELECT id, czy_vr_dostepne FROM Atrakcje").fetchall()

        # Symulacja dnia
        todays_visitors = random.sample(klient_ids, min(len(klient_ids), daily_guests))
        
        for k_id in todays_visitors:
            # Tworzenie wizyty
            bilet = random.choice(bilet_ids)
            cursor.execute("INSERT INTO Wizyty (klient_id, data_wizyty, bilet_id) VALUES (?,?,?)", (k_id, current_date, bilet))
            wizyta_id = cursor.lastrowid
            
            # Kupno ubezpieczeń (szansa 60%)
            if random.random() > 0.4:
                wybrane_ubezp = random.sample(ubezp_ids, k=random.randint(1, 2))
                for u_id in wybrane_ubezp:
                    cursor.execute("INSERT INTO Transakcje_Ubezpieczenia (wizyta_id, ubezpieczenie_id) VALUES (?,?)", (wizyta_id, u_id))
            
            # Korzystanie z atrakcji 
            liczba_atrakcji = random.randint(3, 8)
            for _ in range(liczba_atrakcji):
                atr = random.choice(atrakcje)
                tryb_vr = True if (atr[1] and random.random() > 0.5) else False # Jeśli VR dostępne, to 50% szans
                
                cursor.execute("INSERT INTO Aktywnosci_Atrakcje (wizyta_id, atrakcja_id, czas_rozpoczecia, tryb_vr) VALUES (?,?,?,?)",
                               (wizyta_id, atr[0], current_date, tryb_vr))
                akt_id = cursor.lastrowid
                
                # Wypadek (szansa 1%)
                if random.random() < 0.01:
                    odszkodowanie = round(random.uniform(50, 500), 2)
                    opis = random.choice(["Zgubiony but", "Obrzydzenie VR", "Atak gołębia", "Wylana cola"])
                    cursor.execute("INSERT INTO Incydenty (aktywnosc_id, opis, wyplacone_odszkodowanie) VALUES (?,?,?)",
                                   (akt_id, opis, odszkodowanie))

        # Przeglądy techniczne i awarie (raz na tydzień lub losowo)
        if random.random() < 0.1:
            atr = random.choice(atrakcje)
            koszt = round(random.uniform(200, 2000), 2)
            awaria = random.choice([True, False])
            opis_serwisu = "Wymiana łożysk" if not awaria else "Krytyczny błąd AI"
            cursor.execute("INSERT INTO Przeglady_Techniczne (atrakcja_id, data_przegladu, koszt, czy_awaria, opis) VALUES (?,?,?,?,?)",
                           (atr[0], current_date, koszt, awaria, opis_serwisu))

        current_date += timedelta(days=1)
    
    print("Baza wypełniona danymi.")

# Uruchomienie
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
create_schema(cursor)
populate_data(cursor)
conn.commit()
conn.close()