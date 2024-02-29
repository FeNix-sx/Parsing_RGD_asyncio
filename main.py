import random
import asyncio
import aiohttp
import json
import time
import numpy as np
import pandas as pd

from bs4 import BeautifulSoup

trains = []
city_train_info = dict()


class WorkingWithFile:
    def __init__(self, filename, data: dict=None):
        self.filename = filename
        self.data = data


class FileOpen(WorkingWithFile):
    def json_save(self):
        try:
            with open(self.filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(self.data, indent=4, ensure_ascii=False))

        except Exception as ex:
            print(ex)

    def json_load(self):
        try:
            with open(self.filename, 'r', encoding='utf-8') as file:
                self.data = json.load(file)

            return self.data

        except Exception as ex:
            print(ex)


class TimeConversion():
    """преобразование времени в нужный формат"""
    def __init__(self, data: dict):
        self.data: dict=data

    def __conv_str_to_time(self, str_time: str):
        """из строки в формат времени"""
        time_list = str_time.split()
        nums = [int(part) for part in time_list if part.isdigit()]
        if len(nums) == 3:
            return round(nums[0]*24 + nums[1] + nums[2]/60, 2)
        if len(nums) == 2 and 'дн' in time_list:
            return round(nums[0] * 24 + nums[1]/60, 2)
        else:
            return round(nums[0] + nums[1] / 60, 2)

    def __cont_list_time(self, time_list):
        result = list()

        for item in time_list:
            result.append(self.__conv_str_to_time(item))

        return result

    def convert_time(self):

        for k, v in self.data.items():
            city_train_info[k] = city_train_info.get(k) + [self.__cont_list_time(v[1])]


class CreateExcel:
    def __init__(self, data_str:dict=city_train_info):
        if not data_str:
            self.data_str = city_train_info
        else:
            self.data_str = data_str

    def convert_data(self):
        df_way = pd.DataFrame.from_dict(self.data_str, orient='index').reset_index()
        df_way.columns=["Маршрут", "Номер_поезда", "Время_сайта",  "Источник", "Время_часы"]
        # чтобы убрать пустой список и обработать до конца данные
        df_way["Мин_время"] = df_way["Время_часы"].apply(lambda x: [0, 0] if x == [] else x)
        df_way["Мин_время"] = df_way["Мин_время"].apply(min)

        df_way["Средн_время"] = round(df_way["Время_часы"].apply(lambda x: sum(x) / len(x) if len(x)>0 else 0), 2)
        df_way["В_одну_сторону_СУТ"] = np.ceil(df_way["Средн_время"] / 24)
        df_way["Туда_обратно_СУТ"] = np.ceil(df_way["Средн_время"] * 2 / 24)
        df_way = df_way[
            [
                "Маршрут",
                "В_одну_сторону_СУТ",
                "Туда_обратно_СУТ",
                "Средн_время",
                "Номер_поезда",
                "Время_часы",
                "Время_сайта",
                "Средн_время",
                "Мин_время",
                "Источник"
            ]
        ]

        return df_way

    def write_to_excel(self):
        print('Запись данных в файл .xlsx')

        try:
            result_file_name = f"RESULT.xlsx"
            df_result = self.convert_data()

            with pd.ExcelWriter(result_file_name, engine='xlsxwriter') as writer:
                # создаем лист РЖД_расписание в файле result_file_name и записываем туда df_result
                df_result.to_excel(
                    writer,
                    sheet_name=f"РЖД_расписание",
                    index=False,
                    startrow=0
                )
                # получаем объект workbook и worksheet нужного листа
                workbook = writer.book
                worksheet = writer.sheets[f"РЖД_расписание"]
                # закрепляем первую строку
                worksheet.freeze_panes(1, 0)

                header_style = workbook.add_format({
                    'bg_color': 'black', 'font_color': 'white',
                    'bold': True, 'align': 'center'
                })

                # Задаем заголовок таблицы
                for i, header in enumerate(df_result.columns):
                    worksheet.write(0, i, header, header_style)

                last_row = len(df_result)
                bold_format = workbook.add_format({
                    'bold': True, 'font_color': 'red'
                })
                worksheet.set_row(last_row, None, bold_format)

                del df_result

                print('Информация записана в файл RESULT.xlsx')

        except Exception as ex:
            print(ex)


async def connect_to_site():
    headers = {
        'user_agent': random.choice(FileOpen("user_agent.json").json_load()['user_agents'])
    }
    url = "https://www.ufs-online.ru/raspisanie-poezdov"

    print(f"Подлючение к сайту: {url}")

    async with aiohttp.ClientSession() as session:
        response = await session.get(url=url, headers=headers)
        soup = BeautifulSoup(await response.text(), 'lxml')
        # из общего списка маршрутов оставляем только из москвы (она на сайте вначале), 0-й элемент
        region_moskov = soup.find_all(class_="ufs-accordion__item")[0]
        all_ways_region = region_moskov.find_all(class_="ufs-ways-cities__item")
        # словарь с маршрутами: ключ - название, значение - ссылка
        all_city_dict = dict()

        for item in all_ways_region:
            # если маршрут начинается с "Москва - " (из москвы)
            if "Москва - " in item.findNext('a', href = True).text:
                all_city_dict[item.text] = url+item.findNext('a')['href'][19::].replace(" ","-")

        print(f"Получены маршруты: {len(all_city_dict)}")

        return all_city_dict


async def get_info_of_train(session, url, name, attempt=3):
    headers = {
        'user_agent': random.choice(FileOpen("user_agent.json").json_load()['user_agents'])
    }

    try:
        async with session.get(url=url, headers=headers) as response:
            response = await response.text()
            soup = BeautifulSoup(response, 'lxml')

            # список номеров поездов
            train_list: list = [item.text for item in soup.find_all(class_="sch-schedule-table__name-link")]
            # список времен
            times_list: list = [item.text for item in soup.find_all(class_="sch-schedule-table__lasting")]

            city_train_info[name] = [train_list, times_list, url]

    except Exception as ex:
        if attempt <= 0:
            time.sleep(3)
            await get_info_of_train(session, url, name, attempt-1)


async def gather_data(all_ways: dict):
    print('Cбор информации о поездах')
    tasks = []
    async with aiohttp.ClientSession() as session:

        for key, val in all_ways.items():
            task = asyncio.create_task(get_info_of_train(session=session, url=val, name=key))
            tasks.append(task)

        await asyncio.gather(*tasks)

    time_convert = TimeConversion(city_train_info)
    time_convert.convert_time()


def calculate_execution_time(func):
    """функция-декоратор для вычисления времени работы программы"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения программы: {execution_time} секунд")
        return result
    return wrapper


@calculate_execution_time
def main():
    all_ways = asyncio.run(connect_to_site())
    asyncio.run(gather_data(all_ways))
    excel = CreateExcel()
    excel.write_to_excel()


if __name__ == '__main__':
    main()

