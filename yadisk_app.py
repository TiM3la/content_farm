import yadisk


client_secret = 'ca914f8b0daf4bdf95385ad5d5456dff'
my_token = 'y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg'

y = yadisk.YaDisk(token=my_token)

# Проверяем, валиден ли токен
if y.check_token():
    print("Токен активен!")
else:
    print("Ошибка токена.")

# Выводим названия всех файлов в корне
for item in y.listdir("app:/"):
    if item.type == 'dir':
        print(f"[Папка] {item.name}")
    else:
        print(f"[Файл]  {item.name}")

# Загрузка файла на Диск
y.upload("file_1.txt", "app:/fold_1/file_1_loaded.txt")

# Скачивание файла с Диска
y.download("app:/fold_1/file_1_loaded.txt", "file_1_loaded_2.txt")