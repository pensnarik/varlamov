# varlamov.ru parser

Репозиторий содержит скрипты для анализа постов и фотографий блога varlamov.ru. Данные для
анализа берутся из EXIF изображений, опубликованных в блоге. Приблизительный объем данных
в блоге с 2008 по 2019 года включительно - около 50 Гб.

## Использование

Установите зависимости:

```bash
pip3 install -r requirements.txt
```

Поднимите контейнер с базой данных (PostgreSQL):

```bash
cd database
docker-compose up --build varlamov-db
```

Запустите парсер:

```bash
cd .. && ./get.py --db postgresql://varlamov:varlamov@127.0.0.1:22000/varlamov
```

Для того, чтобы сгенерировать картинки, воспользуйтесь скриптом `render_graphs.py`:

```bash
./render_graphs.py --db postgresql://varlamov:varlamov@127.0.0.1:22000/varlamov
```

## Статистика используемых камер

![Cameras statistic](./graph/cameras.png)

## Статистика по ISO

![ISO statistic](./graph/iso.png)

## Количество постов в блоге

![Posts statistic](./graph/posts.png)

## Зависимости

* Python3
* Pillow
* ExifRead
* parselab
* lxml