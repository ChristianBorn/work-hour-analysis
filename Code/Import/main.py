#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import datetime
import pandas
import time
import traceback


def connect_to_db(db):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    return {'Connection': conn, 'Cursor': c}


def insert_many(table_name, value_list, conn, c):
    if isinstance(value_list, pandas.core.frame.DataFrame):
        values_bracket = '(' + (','.join(['?'] * len(value_list.columns))) + ')'
        value_list = value_list.values.tolist()
    else:
        values_bracket = '(' + (','.join(['?'] * len(value_list))) + ')'

    sql_statement = "INSERT INTO " + table_name + " VALUES " + values_bracket
    c.executemany(sql_statement, value_list)
    conn.commit()


def check_if_exists(row, table_name, value, conn, c):
    sql_statement = "SELECT {row} FROM {table_name} WHERE {row}=?".format(row=row, table_name=table_name)
    c.execute(sql_statement, (value,))
    result = c.fetchone()
    if result is not None:
        c.execute("DELETE FROM {table_name} WHERE {row}=?".format(table_name=table_name, row=row), (result[0],))
        conn.commit()


def process_raw_data(data, connection_details, col_tickets):
    # Clean Ticket-IDs
    ticket_information = pandas.Series(data[col_tickets])
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
    insert_many('buchungen', df_to_import_raw, connection_details['Connection'], connection_details['Cursor'])

    print('Buchungen importiert: {number_entries}'.format(number_entries=len(df_to_import_raw)))
    return df_to_import_raw


def process_ticket_information(df_to_import_raw):
    previous_tickets = pandas.read_sql('SELECT * FROM tickets', sqlite3.connect('../Database/main_data.db'))
    df_to_import_unique_tickets = df_to_import_raw \
        .drop(['monat', 'stunden'], axis=1) \
        .drop_duplicates(subset=['ticketnummer'])

    # Add column for calculated work hours
    df_to_import_unique_tickets['kalkuliert'] = 0.0
    # Find the difference between previous and possibly new tickets, if there are previous tickets
    if not previous_tickets.empty:
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
    extension = datetime.datetime.today()
    extension = str(extension.year)+str(extension.month)+str(extension.day)
    unique_tickets.to_excel('../../Data/Aktuelle_Tickets_{extension}.xlsx'.format(extension=extension),
                            columns=['ticketnummer', 'beschreibung', 'kalkuliert'],
                            float_format="%0.2f")


def import_calculated(connection_details, file_name=''):
    tickets = pandas.read_excel(file_name, convert_float=False, sheet_name='Themenliste + CRs')
    sql_collection = []
    # Todo: Conversion of Comma in floats
    # tickets['kalkuliert'] = tickets['kalkuliert'].to_string()
    # tickets['kalkuliert'] = [x.replace(',', '.') for x in tickets['kalkuliert']]
    # tickets['kalkuliert'].replace(',', '.', inplace=True)
    # pandas.to_numeric(tickets['kalkuliert'])
    # tickets['kalkuliert'] = tickets['kalkuliert'].str.replace(',', '.').astype(float)
    if 'Angeboten Babiel' in tickets.columns:
        tickets['Angeboten Babiel'] = tickets['Angeboten Babiel'].fillna(value=0)
        for index, row in tickets.iterrows():
            if row['Angeboten Babiel'] != 0:
                # Assumption: If there is a JIRA ticket, work will be registered under it;
                # OTRS only for communication purposes
                if not pandas.isna(row['JIRA']):
                    sql_collection.append([row['Angeboten Babiel']*8, row['JIRA']])
                elif not pandas.isna((row['OTRS'])):
                    sql_collection.append([row['Angeboten Babiel']*8, row['OTRS']])
    else:
        tickets['kalkuliert'] = tickets['kalkuliert'].fillna(value=0)
        for index, row in tickets.iterrows():
            sql_collection.append([row['kalkuliert'], row['ticketnummer']])
    sql_statement = "UPDATE tickets SET kalkuliert=? WHERE ticketnummer=?"
    connection_details['Cursor'].executemany(sql_statement, sql_collection)
    connection_details['Connection'].commit()


def analysis(calculated_only=False):

    if calculated_only == True:
        sql_statement = 'SELECT ' \
                        'buchungen.ticketnummer, buchungen.beschreibung, SUM(stunden) as "Bisher geleistet", ' \
                        'kalkuliert,tickets.kalkuliert - SUM(stunden) as "Differenz" ' \
                        'FROM ' \
                        'buchungen,tickets ' \
                        'WHERE ' \
                        'buchungen.ticketnummer=tickets.ticketnummer AND tickets.kalkuliert != 0 ' \
                        'GROUP BY buchungen.ticketnummer '
    else:
        sql_statement = 'SELECT ' \
                        'buchungen.ticketnummer, buchungen.beschreibung, SUM(stunden) as "Bisher geleistet", ' \
                        'kalkuliert,tickets.kalkuliert - SUM(stunden) as "Differenz" ' \
                        'FROM ' \
                        'buchungen,tickets ' \
                        'WHERE ' \
                        'buchungen.ticketnummer=tickets.ticketnummer GROUP BY buchungen.ticketnummer '

    spent_hours = pandas.read_sql(sql_statement, sqlite3.connect('../Database/main_data.db'))
    spent_hours.to_excel('../../Data/Auswertung.xlsx', columns=['ticketnummer', 'beschreibung',
                                                     'Bisher geleistet', 'kalkuliert',
                                                     'Differenz'],
                            float_format="%0.2f")


def start_import(file_name=''):

    connection_details = connect_to_db('../Database/main_data.db')
    while True:
        file_name = input('[?] Unter welchem Pfad liegt die letzte Auswertung?\n')

        if not file_name:
            break
        try:
            data = pandas.read_excel(file_name, sheet_name=0)
            if 'Ticketnummer' in data.columns:
                col_tickets = 'Ticketnummer'
            else:
                col_tickets = 'Hinweis'

            # Drop rows with empty Hinweis
            data.dropna(subset=[col_tickets], inplace=True)

            # Filter for Ticketnumbers only and clean
            data = data[data[col_tickets].str.startswith(('BAB', 'AARE'))]

            # Process raw data and save it for further processing
            df_to_import_raw = process_raw_data(data, connection_details, col_tickets)

            # Process ticket information
            process_ticket_information(df_to_import_raw)
            print('[+] Import erfolgreich abgeschlossen')

            user_input = input('[?] Noch eine Datei? (ja/nein)\n')
            if user_input.lower() != 'ja':
                break
        except FileNotFoundError:
            print('Ungültiger Dateipfad!')

    user_input = input('[?] Soll eine  Aufstellung der aktuellen Tickets erstellt werden? (ja/nein)\n')
    if user_input.lower() == 'ja':
        export_unique_tickets()
        print('[+] Export erstellt!')

    while True:
        user_input = input(
            '[?] Unter welchem Pfad liegen die kalkulierten Tickets? (Falls kein Import gewünscht, Enter drücken)\n')
        if not user_input:
            break
        try:
            import_calculated(connection_details, user_input)
            user_input = input(
                '[?] Soll eine weitere Datei importiert werden? (ja/nein)\n')
            if user_input.lower() != 'ja':
                break
        except FileNotFoundError:
            print('Ungültiger Dateipfad!')
    user_input = input('[?] Soll eine Auswertung erstellt werden? (ja/nein)\n')
    if user_input.lower() == 'ja':
        user_input = input('[?] Nur kalkulierte einbeziehen? (ja/nein)\n')
        if user_input.lower() == 'ja':
            analysis(calculated_only=True)
        else:
            analysis(calculated_only=False)


def main():
    try:
        start_import()
        time.sleep(2)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        while True:
            if not input():
                break

if __name__ == '__main__':
    main()
