**Kolejność i sposób uruchamiania plików aby uzyskać gotowy projekt:**

Aby uruchomić projekt, zainstaluj wymagane biblioteki:

pip install -r wymagane.txt

1. **Generowanie bazy danych:**
   Uruchom skrypt, aby utworzyć plik bazy danych `Projekt_bazy.db` i wypełnić go losowymi danymi:
   
   python generator.py
   
2. **Generowanie raportu z analizą danych:**
Jeżeli nie posiadasz Rscript.exe w zmiennej środowiskowej PATH, to wykonaj następujące kroki:

a) Sprawdź gdzie jest twój Rscript.exe. W systemie Windows zwykle znajduję się w katalogu C:\Program Files\R\R-[wersja]\bin
Np. w moim przypadku jest to C:\Program Files\R\R-4.5.1\bin

b) Następnie dodaj tę ścieżkę do zmiennej środowiskowej PATH. W tym celu wykonaj następujące kroki:

c) W pasku wyszukiwania wpisz: Edytuj zmienne środowiskowe systemu

d) otwórz

e) kliknij zmienne środowiskowe

f) wybierz Path i kliknij edytuj

g) kliknij Nowy, wklej ścieżkę, zatwierdź wszystkie okna i otwórz ponownie terminal

Teraz w celu pobrania wymaganych pakietów należy wpisać w terminalu

Rscript Rwymagane.R

W celu wygenerowania raportu musimy również dodać pandoca do PATH systemowego (jeżeli korzystasz z RStudio to pandoc jest już zainstalowany).
W tym celu wykonaj następujące kroki:

1. W konsoli RStudio wykonaj polecenie: rmarkdown::find_pandoc()
2. Teraz dodaj znalezioną ścieżkę do zmiennej środowiskowej PATH (tak samo jak poprzednio). UWAGA: Pamiętaj, aby / zamienić na \
Następnie żeby wygenerować raport należy napisać w terminalu

Rscript -e "rmarkdown::render('Analiza_danych.Rmd')"

   **Spis użytych technologii:**
   
   1. R 4.5.1 oraz RStudio jako środowisko programistyczne w części 3 i 4.
   
   **Lista plików i opis ich zawartości**
   
   1. Plik Analiza_danych.Rmd zawiera przeprowadzoną analizę danych gotową do wygenerowania w formie raportu.
   2. Plik Analiza_danych.html zawiera analizę danych wygenerowaną z pliku Analiza_danych.Rmd w formie raportu HTML.
   3. Plik Rwymagane.R skrypt w języku R, służący do instalacji pakietów potrzebnych do wykonania części 3 i 4 projektu.
   
   

   Tworzenie schematu:
   -

   Uzupełnianie skryptowo bazy danymi:
   - Python 3.12.9 - główny język programu
   - Mimesis - generowanie realistycznych danych testowych (locale: PL)
   - SQLite - obsługa bazy danych
