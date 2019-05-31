#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import pandas
import sqlite3
import datetime

def insert_values_into(table_name, value_list, conn, c):
    values_bracket = '('+(','.join(['?']*len(value_list)))+')'
    sql_statement = "INSERT INTO "+table_name+" VALUES "+values_bracket
    c.execute(sql_statement, value_list)
    conn.commit()


def start_import(file_name=''):
    more_data = True
    while more_data == True:
        file_name = input('[?] Unter welchem Pfad liegt die letzte Auswertung?\n')
        data = pandas.read_excel(file_name, sheet_name='Tabelle1')
        # Drop rows with empty Hinweis
        data.dropna(subset=['Hinweis'], inplace=True)

        # Filter for Ticketnumbers only and clean
        data = data[data['Hinweis'].str.startswith(('BAB', 'AARE'))]
        ticket_information = pandas.Series(data['Hinweis'])
        ticket_information = ticket_information.str.strip()
        ticket_information = ticket_information.str.replace('  ', ' ')
        ticket_information = ticket_information.str.split(' ', n=1, expand=True)
        ticket_information[0] = ticket_information[0].str.replace(':', '')

        # Convert Dates to datetime and extract month and year
        data['Datum'] = pandas.to_datetime(data['Datum']).dt.to_period('M')

        # Create Dataframe to import into database
        df_to_import_raw = pandas.DataFrame({'ticketnummer': ticket_information[0].tolist(),
                                         'beschreibung': ticket_information[1].tolist(),
                                         'monat': data['Datum'].astype(str),
                                         'stunden': data['Geleistete\nStunden']
                                         })


        df_to_import_unique_tickets = df_to_import_raw\
            .drop(['monat', 'stunden'], axis=1)\
            .drop_duplicates(subset=['ticketnummer'])
        df_to_import_unique_tickets.to_sql(name='tickets', con=sqlite3.connect('../Database/main_data.db'), if_exists='replace')
        df_to_import_raw.to_sql(name='buchungen', con=sqlite3.connect('../Database/main_data.db'), if_exists='replace')
        #print(df_to_import_unique_tickets)
        print('[+] Import erfolgreich abgeschlossen')
        user_input = input('[?] Noch eine Datei? (ja/nein)\n')
        if user_input.lower() != 'ja':
            more_data = False








def main():
    start_import()


if __name__ == '__main__':
    main()