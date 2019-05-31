#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3


def create_tables():
    conn = sqlite3.connect('main_data.db')
    c = conn.cursor()
    c.execute('''DROP TABLE if exists tickets''')
    print('[+] Creating table tickets')
    c.execute('''CREATE TABLE tickets
                        (ticketnummer TEXT,
                        beschreibung TEXT,
                        kalkuliert REAL DEFAULT 0
                        )''')
    c.execute('''DROP TABLE if exists buchungen''')
    print('[+] Creating table buchungen')
    c.execute('''CREATE TABLE buchungen
                (ticketnummer TEXT,
                beschreibung TEXT,
                monat TEXT,
                stunden REAL,
                FOREIGN KEY(ticketnummer) REFERENCES tickets(ticketnummer) ON DELETE CASCADE
                )''')
    conn.commit()
    conn.close()


def main():
    create_tables()


if __name__ == '__main__':
    main()
