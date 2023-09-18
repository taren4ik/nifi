import re
import sys

import pandas as pd

fio = []
born = []
name_pattern = r'([А-ЯЁ][а-яё]+[\-\s]?){3,}'
born_pattern = r'(\d{1,2})\.(\d{2})\.(\d{4})'

text_all = sys.stdin
# try:
#     text_all = sys.stdin
# except Exception as e:
#     assert ('ошибка чтения потока', e)


def extart_docx(text_all):
    """Извлекает ФИО и дату рождения
    :param text_all: текст из потока.
    :return df: ФИО+ дата рождения."""

    for line in text_all:
        if re.search(name_pattern, line):
            person = re.search(name_pattern, line)
            fio.append(person.group(0))
        if re.search(born_pattern, line):
            dt_born = re.search(born_pattern, line)
            born.append(dt_born.group(0))

    df = pd.DataFrame(
        {'fio': fio,
         'born': born})
    df.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    extart_docx(text_all)
