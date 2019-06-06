#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3

import pandas


def connect_to_db(db):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    return {'Connection': conn, 'Cursor': c}


def insert_values_into(table_name, value_list, conn, c):
    values_bracket = '(' + (','.join(['?'] * len(value_list))) + ')'
    sql_statement = "INSERT INTO " + table_name + " VALUES " + values_bracket
    c.execute(sql_statement, value_list)
    conn.commit()


def check_if_exists(row, table_name, value, conn, c):
    sql_statement = "SELECT {row} FROM {table_name} WHERE {row}=?".format(row=row, table_name=table_name)
    c.execute(sql_statement, (value,))
    result = c.fetchone()
    if result is not None:
        c.execute("DELETE FROM {table_name} WHERE {row}=?".format(table_name=table_name, row=row), (result[0],))
        conn.commit()


def process_raw_data(data, connection_details):
    # Clean Ticket-IDs
    ticket_information = pandas.Series(data['Hinweis'])
    ticket_information = ticket_information.str.strip()
    ticket_information = ticket_information.str.replace('  ', ' ')
    ticket_information = ticket_information.str.split(' ', n=1, expand=True)
    ticket_information[0] = ticket_information[0].str.replace(':', '')
    # Convert Dates to datetime and extract month and year
    data['Datum'] = pandas.to_datetime(data['Datum'], format='%d.%m.%Y').dt.to_period('M')

    # Create two Dataframes to import into database
    df_to_import_raw = pandas.DataFrame({'ticketnummer': ticket_information[0].tolist(),
                                         'beschreibung': ticket_information[1].tolist(),
                                         'monat': data['Datum'].astype(str),
                                         'stunden': data['Geleistete\nStunden']
                                         })
    check_if_exists('monat', 'buchungen', df_to_import_raw['monat'].iat[0], connection_details['Connection'],
                    connection_details['Cursor'])
    for index, row in df_to_import_raw.iterrows():
        insert_values_into('buchungen', row.to_list(), connection_details['Connection'], connection_details['Cursor'])
    print('Buchungen importiert: {number_entries}'.format(number_entries=len(df_to_import_raw)))
    return df_to_import_raw


def process_ticket_information(df_to_import_raw):
    previous_tickets = pandas.read_sql('SELECT * FROM tickets', sqlite3.connect('../Database/main_data.db'))
    df_to_import_unique_tickets = df_to_import_raw \
        .drop(['monat', 'stunden'], axis=1) \
        .drop_duplicates(subset=['ticketnummer'])

    # Add column for calculated work hours
    df_to_import_unique_tickets['kalkuliert'] = 0.0
    # Find the difference between previous and possibly new tickets
    df_to_import_unique_tickets = pandas.concat([df_to_import_unique_tickets, previous_tickets], ignore_index=True,
                                                sort=False) \
        .drop_duplicates(subset=['ticketnummer']).drop('index', axis=1)

    df_to_import_unique_tickets.to_sql(name='tickets', con=sqlite3.connect('../Database/main_data.db'),
                                       if_exists='replace')
    print('Zahl neuer Tickets: {new_tickets}'.format(
        new_tickets=len(df_to_import_unique_tickets) - len(previous_tickets)))


def export_unique_tickets():
    unique_tickets = pandas.read_sql('SELECT * FROM tickets', sqlite3.connect('../Database/main_data.db'))
    unique_tickets.drop('index', axis=1)
    unique_tickets.to_excel('Aktuelle_Tickets.xlsx', columns=['ticketnummer', 'beschreibung', 'kalkuliert'],
                            float_format="%0.2f")


def import_calculated(connection_details, file_name=''):
    tickets = pandas.read_excel(file_name, usecols=[1, 2, 3], convert_float=False)
    # Todo: Conversion of Comma in floats
    # tickets['kalkuliert'] = tickets['kalkuliert'].to_string()
    # tickets['kalkuliert'] = [x.replace(',', '.') for x in tickets['kalkuliert']]
    # tickets['kalkuliert'].replace(',', '.', inplace=True)
    # pandas.to_numeric(tickets['kalkuliert'])
    # tickets['kalkuliert'] = tickets['kalkuliert'].str.replace(',', '.').astype(float)

    sql_collection = []
    for index, row in tickets.iterrows():
        sql_collection.append([row['kalkuliert'], row['ticketnummer']])
    sql_statement = "UPDATE tickets SET kalkuliert=? WHERE ticketnummer=?"
    connection_details['Cursor'].executemany(sql_statement, sql_collection)
    connection_details['Connection'].commit()


def analysis():
    spent_hours = pandas.read_sql(
        'SELECT '
        'buchungen.ticketnummer, buchungen.beschreibung, SUM(stunden) as "Bisher geleistet", '
        'kalkuliert,tickets.kalkuliert - SUM(stunden) as "Differenz" '
        'FROM '
        'buchungen,tickets '
        'WHERE '
        'buchungen.ticketnummer=tickets.ticketnummer GROUP BY buchungen.ticketnummer ',
        sqlite3.connect('../Database/main_data.db'))
    spent_hours.to_excel('Auswertung.xlsx', columns=['ticketnummer', 'beschreibung',
                                                     'Bisher geleistet', 'kalkuliert',
                                                     'Differenz'],
                            float_format="%0.2f")


def start_import(file_name=''):
    more_data = True
    connection_details = connect_to_db('../Database/main_data.db')
    while more_data == True:
        file_name = input('[?] Unter welchem Pfad liegt die letzte Auswertung?\n')
        if not file_name:
            break
        data = pandas.read_excel(file_name, sheet_name=0)

        # Drop rows with empty Hinweis
        data.dropna(subset=['Hinweis'], inplace=True)

        # Filter for Ticketnumbers only and clean
        data = data[data['Hinweis'].str.startswith(('BAB', 'AARE'))]

        # Process raw data and save it for further processing
        df_to_import_raw = process_raw_data(data, connection_details)

        # Process ticket information
        process_ticket_information(df_to_import_raw)

        print('[+] Import erfolgreich abgeschlossen')
        user_input = input('[?] Noch eine Datei? (ja/nein)\n')
        if user_input.lower() != 'ja':
            more_data = False
    user_input = input('[?] Soll eine  Aufstellung der aktuellen Tickets erstellt werden? (ja/nein)\n')
    if user_input == 'ja':
        export_unique_tickets()
        print('[+] Export erstellt!')
    user_input = input(
        '[?] Unter welchem Pfad liegen die kalkulierten Tickets? (Falls kein Import gewünscht, Enter drücken)\n')
    if user_input:
        import_calculated(connection_details, user_input)
    user_input = input('[?] Soll eine Auswertung erstellt werden? (ja/nein)\n')
    if user_input == 'ja':
        analysis()


def main():
    start_import()


if __name__ == '__main__':
    main()
