    dest = dict_value.get('dest', '')
    reg = dict_value.get('reg', '')

    # Pomijaj dokumenty z pustymi wartościami dest lub reg
    if not dest or not reg:
        continue

    # Konwersja pitr na int
    try:
        pitr = int(pitr)
    except ValueError:
        continue

    # Sprawdzenie, czy taki sam numer rejestracyjny już istnieje w zbiorze przetworzonych lotów
    if (id, ident, reg) in processed_regs:
        continue

    # Jeśli nie istnieje, dodaj do zbioru przetworzonych lotów
    processed_regs.add((id, ident, reg))

    # Dodaj dokument do listy znalezionych dokumentów
    found_documents.append(dict_value)

    # Przygotowanie danych do serializacji
    output = {
        "id": id,
        "orig": orig,
        "dest": dest,
        "ident": ident,
        "reg": reg,
        "pitr": convert_epoch_time(pitr)
    }

    unique_flights.append(output)
end_processing = time.time()

# Serializacja do JSON i zapis do pliku w jednym kroku
start_file_write = time.time()
with codecs.open('ident_data.json', 'w', encoding='utf-8') as file:
    for flight in unique_flights:
        json_document = json.dumps(flight, cls=JSONEncoder, ensure_ascii=False)
        file.write(json_document + '\n')
end_file_write = time.time()

# Pomiar czasu zakończenia
end_total = time.time()

print("Zakończono zapisywanie danych do pliku.")

# Otwarcie pliku JSON i wypisanie jego zawartości na konsoli
start_file_read = time.time()
with codecs.open('ident_data.json', 'r', encoding='utf-8') as file:
    for line in file:
        line = line.strip()
        if line:
            print(line)
end_file_read = time.time()

# Wyświetlenie czasu wykonania poszczególnych części
print("Czas połączenia z bazą danych: {:.2f} sekund".format(end_db_connection - start_db_connection))
print("Czas zapytania do bazy danych: {:.2f} sekund".format(end_query - start_query))
print("Czas przetwarzania danych: {:.2f} sekund".format(end_processing - start_processing))
print("Czas zapisu danych do pliku: {:.2f} sekund".format(end_file_write - start_file_write))
print("Czas odczytu danych z pliku: {:.2f} sekund".format(end_file_read - start_file_read))
print("Całkowity czas wykonania: {:.2f} sekund".format(end_total - start_total))

an467298@fsig-students:~/ProjektLotniczy/ONS$ clear
an467298@fsig-students:~/ProjektLotniczy/ONS$ cat loty_polska_przyspieszony_v2.py
# -*- coding: utf-8 -*-
import pymongo
import time
import json
import codecs
import datetime
from bson import ObjectId

# Klasa do serializacji obiektów ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

# Funkcja do konwersji czasu epoch na format czytelny dla człowieka
def convert_epoch_time(epoch_time):
    dt = datetime.datetime.fromtimestamp(epoch_time)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# Funkcja do konwersji daty w formacie YYYY-MM-DD na epoch
def convert_date_to_epoch(date_str):
    dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    epoch_time = time.mktime(dt.timetuple())
    return int(epoch_time)

# Pomiar czasu rozpoczęcia
start_total = time.time()

# Połączenie z bazą danych MongoDB
start_db_connection = time.time()
client = pymongo.MongoClient('mongodb://student:student@10.20.66.7/fsig-raw')
db = client['fsig-raw']
collection = db['rawFaSignals']
end_db_connection = time.time()

# Konwersja dat na epoch
while True:
    start_date = raw_input("Podaj początkową datę (YYYY-MM-DD): ")
    try:
        start_epoch = convert_date_to_epoch(start_date)
        break
    except ValueError:
        print("Nieprawidłowy format daty. Podaj datę w formacie YYYY-MM-DD.")

while True:
    end_date = raw_input("Podaj końcową datę (YYYY-MM-DD): ")
    try:
        end_epoch = convert_date_to_epoch(end_date)
        if end_epoch > start_epoch:
            break
        else:
            print("Data końcowa musi być późniejsza niż data początkowa.")
    except ValueError:
        print("Nieprawidłowy format daty. Podaj datę w formacie YYYY-MM-DD.")

# Kryteria wyszukiwania dokumentów w określonym przedziale czasowym
date_criteria = {
    "$and": [
        {"value.pitr": {"$gte": str(start_epoch)}},
        {"value.pitr": {"$lt": str(end_epoch)}},
        {"value.orig": "EPWA"}
    ]
}

# Znajdź wszystkie dokumenty spełniające kryteria czasowe i wybrane pola
start_query = time.time()
projection = {"value.id": 1, "value.ident": 1, "value.pitr": 1, "value.orig": 1, "value.dest": 1, "value.reg": 1}
documents_in_date_range = list(collection.find(date_criteria, projection))
end_query = time.time()

# Wyświetlenie liczby znalezionych dokumentów w zakresie dat
print("Liczba znalezionych dokumentów w zakresie dat {} - {}: {}".format(start_date, end_date, len(documents_in_date_range)))

# Przetwarzanie dokumentów i filtrowanie unikalnych lotów
start_processing = time.time()
processed_regs = set()
unique_flights = []
found_documents = []

for document in documents_in_date_range:
    dict_value = document.get('value', {})
    id = dict_value.get('id', '')
    ident = dict_value.get('ident', '')
    pitr = dict_value.get('pitr', '')
    orig = dict_value.get('orig', '')
    dest = dict_value.get('dest', '')
    reg = dict_value.get('reg', '')

    # Pomijaj dokumenty z pustymi wartościami dest lub reg
    if not dest or not reg:
        continue

    # Konwersja pitr na int
    try:
        pitr = int(pitr)
    except ValueError:
        continue

    # Sprawdzenie, czy taki sam numer rejestracyjny już istnieje w zbiorze przetworzonych lotów
    if (id, ident, reg) in processed_regs:
        continue

    # Jeśli nie istnieje, dodaj do zbioru przetworzonych lotów
    processed_regs.add((id, ident, reg))

    # Dodaj dokument do listy znalezionych dokumentów
    found_documents.append(dict_value)

    # Przygotowanie danych do serializacji
    output = {
        "id": id,
        "orig": orig,
        "dest": dest,
        "ident": ident,
        "reg": reg,
        "pitr": convert_epoch_time(pitr)
    }

    unique_flights.append(output)
end_processing = time.time()

# Serializacja do JSON i zapis do pliku w jednym kroku
start_file_write = time.time()
with codecs.open('ident_data.json', 'w', encoding='utf-8') as file:
    for flight in unique_flights:
        json_document = json.dumps(flight, cls=JSONEncoder, ensure_ascii=False)
        file.write(json_document + '\n')
end_file_write = time.time()

# Pomiar czasu zakończenia
end_total = time.time()

print("Zakończono zapisywanie danych do pliku.")

# Otwarcie pliku JSON i wypisanie jego zawartości na konsoli
start_file_read = time.time()
with codecs.open('ident_data.json', 'r', encoding='utf-8') as file:
    for line in file:
        line = line.strip()
        if line:
            print(line)
end_file_read = time.time()

# Wyświetlenie czasu wykonania poszczególnych części
print("Czas połączenia z bazą danych: {:.2f} sekund".format(end_db_connection - start_db_connection))
print("Czas zapytania do bazy danych: {:.2f} sekund".format(end_query - start_query))
print("Czas przetwarzania danych: {:.2f} sekund".format(end_processing - start_processing))
print("Czas zapisu danych do pliku: {:.2f} sekund".format(end_file_write - start_file_write))
print("Czas odczytu danych z pliku: {:.2f} sekund".format(end_file_read - start_file_read))
print("Całkowity czas wykonania: {:.2f} sekund".format(end_total - start_total))

an467298@fsig-students:~/ProjektLotniczy/ONS$