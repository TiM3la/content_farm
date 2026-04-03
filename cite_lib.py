import duckdb as d
import yadisk as yd
import numpy as np
import pandas as pd
import os, mmap, codecs


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
            self.base.execute('create table if not exists Book (id integer primary key, title varchar(255), author varchar(255), language varchar(2), date_yd TIMESTAMP_S, position integer, is_readed boolean, link_yd varchar(255))')
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
                        self.base.execute('insert into Book (id, title, author, language, size, date_yd, position, is_readed, link_yd) values(?, ?, ?, ?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, 0, False, ?)', [int(id), title, author, language, None, f'app:/books/{item.name}'])
                        print(f'[ОК] Загружена книга {item.name}')
                    else:
                        print(f'[ОК] Книга {item.name} была загружена ранее')
                else:
                    print(f'[?] В папке нет книг')
        except Exception as e:
            print(f'[!] Ошибка! Книги не загружены. {e}')

    def make_book_fragment(self):

        def read_fragment_approx(file_path, byte_pos, chunk_bytes, encoding='utf-8'):
            """
            Читает chunk_bytes байт из файла начиная с byte_pos.
            Возвращает (текст, следующая_байтовая_позиция).
            """
            if byte_pos < 0:
                byte_pos = 0
            with open(file_path, 'rb') as f:
                f.seek(byte_pos)
                data = f.read(chunk_bytes)
                new_pos = f.tell()
                text = data.decode(encoding, errors='replace')
            return text, new_pos

        try:
            print(f'[Процесс...] Извлекаем фрагмент из книги')
            df = self.base.execute('SELECT * FROM Book WHERE is_readed IS FALSE ORDER BY id LIMIT 1').df()
            if df.empty:
                print(f'[Ошибка] Все книги прочитанны')
                return
            book_object = df.iloc[0].to_dict()

            file_name = book_object['link_yd'].split('/')[-1]
            if not os.path.exists(f'books/{file_name}'):
                self.y.download(book_object['link_yd'], f'books/{file_name}')

            text, next_byte = read_fragment_approx(f'books/{file_name}', book_object['position']-fragment_overlap * 2, fragment_size * 2)
            if text:
                cur_fragment = self.base.execute(
                    'insert into Fragment(book_id, size, date_yd, link_yd) values (?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, ?) RETURNING id',
                    [book_object['id'], len(text), None])
                fragment_id = cur_fragment.fetchone()[0]
                with open(f'fragments/{fragment_id}_{book_object["title"]}.txt', 'w',
                          encoding='utf-8') as fragment_file:
                    fragment_file.write(text)
                self.base.execute('update Fragment set link_yd = ? where id = ?',
                                  [f'app:/fragments/{fragment_id}_{book_object["title"]}.txt', fragment_id])
                self.y.upload(f'fragments/{fragment_id}_{book_object["title"]}.txt',
                              f'app:/fragments/{fragment_id}_{book_object["title"]}.txt')
                os.remove(f'fragments/{fragment_id}_{book_object["title"]}.txt')
                print(f'[ОК] Получен фрагмент {fragment_id}')
                self.base.execute('update Book set position = ? where id = ?', [next_byte, book_object['id']])
            else:
                self.base.execute('update Book set is_readed = True where id = ?', [book_object['id']])
                print(f'[ОК] Книга прочитана до конца')
                os.remove(f'books/{file_name}')
        except Exception as e:
            print(f'[Ошибка] Фрагмент не получен: {e}')


if __name__ == '__main__':
    main_db = Main_DB(db_name, yd_token)
    main_db.load_books()
    for i in range(3):
        main_db.make_book_fragment()