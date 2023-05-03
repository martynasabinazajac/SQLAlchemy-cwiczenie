import csv
import sqlite3
from sqlite3 import Error
from sqlalchemy import create_engine, Table, Column, Integer, MetaData, String

# 1. Pobranie csv i przygotowanied anych z pliku:


# pobranie danych z pliku csv
def pobrranie_csv(plikcsv, delimeter=","):
    file = open(plikcsv, "r")
    dataframe = {}
    headers = file.readline().replace("\n", "").split(delimeter)
    for line in file:
        linedata = line.replace("\n", "").split(delimeter)
        row = {}
        for h in range(len(headers)):
            if h in range(len(linedata)):
                row[headers[h]] = linedata[h]
            else:
                row[headers[h]] = "null"
        dataframe[linedata[0]] = row
    file.close()
    return dataframe

    # pobranie kluczy i wartości potrzebnych do stworzenia tabeli:


def pobranie_danych(data):
    lists = []
    for l in data:
        headers_list = []
        for h in data[l].keys():
            headers_list.append(h)
    lists.append(headers_list)
    for r in data.values():
        lists.append(r)
    return lists


# Krok 2 Utworzenie bazy gdzie będa trafiać dane:


# utworzenie bazy
def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


# Krok 3 Dodanie danych z csv do tabel


# dodanie danych do tabel
def dodanie_danych_do_tabel(tabela, dane):
    conn = engine.connect()
    ins = tabela.insert().values()
    result = conn.execute(ins, dane)
    return result


if __name__ == "__main__":
    # pobranie danych z csv
    clean_measure = pobrranie_csv("clean_measure.csv", delimeter=",")
    clean_station = pobrranie_csv("clean_stations.csv", delimeter=",")

    # pobranie nazw kluczy i wartości
    clean_measure_data = pobranie_danych(clean_measure)
    clean_station_data = pobranie_danych(clean_station)
    print(clean_measure_data[0:], clean_station_data[0])

    # utworzenie bazy (piiku, gdzie będa wzucane dane)
    db_file = "alchemysqlćwiczenia.db"
    conn = create_connection(db_file)
    conn.close()

    # połaczenie i utworzenie tabel za pomocą sql alchemy
    engine = create_engine("sqlite:///alchemysqlćwiczenia.db")

    meta = MetaData()

    measure = Table(
        "clean_measure",
        meta,
        Column("station", String, primary_key=True),
        Column("date", String),
        Column("precip", String),
        Column("tobs", String),
    )

    station = Table(
        "clean_station",
        meta,
        Column("station", String, primary_key=True),
        Column("latitude", String),
        Column("longitude", String),
        Column("elevation", String),
        Column("name", String),
        Column("country", String),
        Column("state", String),
    )

    meta.create_all(engine)
    print(engine.table_names())

    # dodanie danych do tabe- instert
    dodanie_danych_do_tabel(measure, clean_measure_data[1:])
    dodanie_danych_do_tabel(station, clean_station_data[1:])

    # pobranie wszystkiego
    print("odczyt wszystkiego do LIMIT 5 z bazy")
    all = engine.execute("SELECT * FROM clean_station LIMIT 5").fetchall()
    all2 = engine.execute("SELECT * FROM clean_measure LIMIT 5").fetchall()
    print(all, all2)

    # pobranie przykładowe kilka rzeczy
    r = measure.select().where(measure.c.date > "2017-07-31")
    result = engine.execute(r)
    print("Odczyt z bazy")
    for row in result:
        print(row)

    # zmiana przykładowa:
    u = (
        measure.update()
        .where(measure.c.station == "USC00514830")
        .values(precip="100.11")
    )
    result = engine.execute(u)
    # pobranie w celu weryfikacji
    r = measure.select().where(measure.c.station == "USC00514830")
    result = engine.execute(r)
    for i in result:
        print("modyfikacja danych", i)

    # przykładowe usunięcie
    d = measure.delete().where(measure.c.station == "USC00514830")
    result = engine.execute(d)
    # pobranie w celu weryfikacji
    r = measure.select().where(measure.c.station == "USC00514830")
    result = engine.execute(r)
    if i in result:
        print("nie usunięto", i)
    else:
        print("usunięto")
