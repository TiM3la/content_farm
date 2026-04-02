import duckdb as d
import yadisk as yd
import numpy as np
import pandas as pd
import os

yd_token = 'y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg'
db_name = 'main_db.duckdb'
fragment_size = 4000
fragment_overlap = 300

class Main_DB:
    def __init__(self, db_name, yd_token):
        self.base = d.connect(db_name)
        self.y = yd.YaDisk(token=yd_token)

        print(self.make_book_table())
        print(self.make_fragment_table())

    def make_book_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Book')
            self.base.execute('create table if not exists Book (id integer primary key, title varchar(255), author varchar(255), language varchar(2), size integer, date_yd TIMESTAMP_S, date_fragment TIMESTAMP_S, link_yd varchar(255))')
            return '[ОК] Таблица Book создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Book не создана. {e}'

    def make_fragment_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Fragment')
            self.base.execute("CREATE SEQUENCE IF NOT EXISTS seq_fragment_id START 1;")
            self.base.execute("create table if not exists Fragment (id integer primary key default nextval('seq_fragment_id'), book_id integer REFERENCES Book(id), size integer, date_yd TIMESTAMP_S, link_yd varchar(255))")
            return '[ОК] Таблица Fragment создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Fragment не создана. {e}'

    def load_books(self):
        try:
            print(f'[Процесс...] Загружаем книги')
            for item in self.y.listdir("app:/books"):
                if item.name.endswith('.txt'):
                    id, title, author, language = item.name.rstrip('.txt').split('_')
                    result = self.base.execute("select * from Book where id = ?", [id]).fetchall()
                    if not bool(result):
                        self.base.execute('insert into Book (id, title, author, language, size, date_yd, date_fragment, link_yd) values(?, ?, ?, ?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, ?, ?)', [int(id), title, author, language, None, None, f'app:/books/{item.name}'])
                        print(f'[ОК] Загружена книга {item.name}')
                    else:
                        print(f'[ОК] Книга {item.name} была загружена ранее')
                else:
                    print(f'[?] В папке нет книг')
        except Exception as e:
            print(f'[!] Ошибка! Книги не загружены. {e}')

    def fragment_books(self):
        try:
            print(f'[Процесс...] Фрагментируем книги')
            result = self.base.execute('select id from Book where date_fragment is NULL').fetchall()
            if result:
                print(f'[Процесс...] Не фрагментированные книги: {result}')
                for book_id in result:
                    book_object = self.base.execute('select * from Book where id = ?', [book_id[0]]).df().iloc[0].to_dict()
                    print(f'[Процесс...] Фрагментирую книгу: {book_object}')
                    file_name = book_object['link_yd'].split('/')[-1]
                    self.y.download(book_object['link_yd'], f'books/{file_name}')
                    fragment_0 = ''
                    book_size = 0
                    with open(f'books/{file_name}', 'r', encoding='utf-8') as book_file:
                        while True:
                            fragment_1 = book_file.read(fragment_size - len(fragment_0))
                            book_size += len(fragment_1)
                            if not fragment_1:
                                break
                            fragment_1 = fragment_0 + fragment_1
                            # print(fragment_1)
                            cur_fragment = self.base.execute('insert into Fragment(book_id, size, date_yd, link_yd) values (?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, ?) RETURNING id', [book_id[0], len(fragment_1), None])
                            fragment_id = cur_fragment.fetchone()[0]
                            with open(f'fragments/{fragment_id}_{book_object["title"]}.txt', 'w', encoding='utf-8') as fragment_file:
                                fragment_file.write(fragment_1)
                            self.base.execute('update Fragment set link_yd = ? where id = ?', [f'app:/fragments/{fragment_id}_{book_object["title"]}.txt', fragment_id])
                            self.y.upload(f'fragments/{fragment_id}_{book_object["title"]}.txt', f'app:/fragments/{fragment_id}_{book_object["title"]}.txt')
                            os.remove(f'fragments/{fragment_id}_{book_object["title"]}.txt')
                            fragment_0 = fragment_1[-fragment_overlap:]

                        self.base.execute('update Book set size = ?, date_fragment = NOW()::TIMESTAMP::TIMESTAMP_S where id = ?', [book_size, book_id[0]])
                        print(f'[ОК] Книга {book_object["title"]} фрагментирована. Объем: {book_size}')
                    os.remove(f'books/{file_name}')
            else:
                print(f'[ОК] Все книги были фрагментированы ранее')
        except Exception as e:
            print(f'[Ошибка] Ошибка фрагментации книги: {e}')



if __name__ == '__main__':
    # подключение к базе данных
    main_db = Main_DB(db_name, yd_token)
    main_db.load_books()
    main_db.fragment_books()