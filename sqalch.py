import csv
import pandas as pd
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    MetaData,
    String,
    Date,
    Float,
)


# 1. Pobranie csv i przygotowanied anych z pliku:
def pobieranie_pandas(plik):
    df = pd.read_csv(plik)
    df = df.to_dict("records")
    return df


# Krok 3 Dodanie danych z csv do tabel


# dodanie danych do tabel
def dodanie_danych_do_tabel(tabela, dane, engine):
    conn = engine.connect()
    ins = tabela.insert().values()
    result = conn.execute(ins, dane)
    return result


if __name__ == "__main__":
    # pobranie danych z csv
    clean_measure_data = pobieranie_pandas("clean_measure.csv")
    clean_station_data = pobieranie_pandas("clean_stations.csv")
    print(clean_measure_data, clean_station_data)

    # Krok 2 połaczenie i utworzenie tabel za pomocą sql alchemy
    engine = create_engine("sqlite:///alchemysqlćwiczenia.db")

    meta = MetaData()

    measure = Table(
        "clean_measure",
        meta,
        Column("id", Integer, primary_key=True),
        Column("station", String),
        Column("date", String),
        Column("precip", Float),
        Column("tobs", Integer),
    )

    station = Table(
        "clean_station",
        meta,
        Column("station", String, primary_key=True),
        Column("latitude", Float),
        Column("longitude", Float),
        Column("elevation", Float),
        Column("name", String),
        Column("country", String),
        Column("state", String),
    )

    meta.create_all(engine)
    print(engine.table_names())

    # dodanie danych do tabe- instert
    dodanie_danych_do_tabel(measure, clean_measure_data, engine)
    dodanie_danych_do_tabel(station, clean_station_data, engine)

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
    for r in result:
        print("modyfikacja danych:", r)

    # przykładowe usunięcie
    d = measure.delete().where(measure.c.id == 2)
    result = engine.execute(d)
    # pobranie w celu weryfikacji
    r = measure.select().where(measure.c.station == "USC00514830")
    result = engine.execute(r)
    if r in result:
        print("nie usunięto", r)
    else:
        print("usunięto")
