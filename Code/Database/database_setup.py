#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3

#todo: Enforce Foreign KEys
def create_tables():
    conn = sqlite3.connect('main_data.db')
    c = conn.cursor()
    c.execute('''DROP TABLE if exists tickets''')
    print('[+] Creating table tickets')
    c.execute('''CREATE TABLE tickets
                        (ticketnummer TEXT,
                        beschreibung TEXT,
                        kalkuliert REAL NOT NULL DEFAULT 0
                        )''')
    c.execute('''DROP TABLE if exists buchungen''')
    print('[+] Creating table buchungen')
    c.execute('''CREATE TABLE buchungen
                (ticketnummer TEXT,
                beschreibung TEXT,
                monat TEXT,
                stunden REAL
                )''')
    conn.commit()
    conn.close()


def main():
    create_tables()


if __name__ == '__main__':
    main()
