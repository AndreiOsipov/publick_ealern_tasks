import datetime
import csv
import multiprocessing as mp
import time
from pathlib import Path

class Translator:
    currency_to_rub = {  
        "AZN": 35.68,  
        "BYR": 23.91,  
        "EUR": 59.90,  
        "GEL": 21.74,  
        "KGS": 0.76,  
        "KZT": 0.13,  
        "RUR": 1,  
        "UAH": 1.64,  
        "USD": 60.66,  
        "UZS": 0.0055,  
    }
class Salary:
    '''
    представляет собой зарплату
    основное поле для взаимодействия -- rur_middle_salary
    (усредненная зп в рублях)
    '''
    def __init__(self, salary_from, salary_to, salary_currency) -> None:
        self.salary_from = salary_from
        self.salary_to = salary_to
        self.salary_currency = salary_currency
        self.rur_middle_salary = round((salary_from + salary_to) / 2 * Translator.currency_to_rub[salary_currency])

class InputConnect:
    '''
    хранит имя папки и название профессии
    '''
    def __init__(self):
        self.file_name = input('Введите название папки: ')
        self.profession = input('Введите название профессии: ')

class YearsVacancy:
    '''
    представляет вакансию для сбора ститистики по годам
    '''
    def __init__(self, vac_name: str, vac_salary: Salary, published_year) -> None:
        self.vac_name = vac_name
        self.vac_salary = vac_salary
        self.published_year = published_year

class CitiesVacancy:
    '''
    прдеставляет вакансию для сбора статистики по городам
    '''
    def __init__(self, city_name: str, vac_salary: Salary) -> None:
        self.city_name = city_name
        self.vac_salary = vac_salary

class CitiesStat:
    '''
    статистика зарплат погородам, которая собирается из city_vac_list -- 
    списка вакансий CitiesVacancy

    '''
    def __init__(self, city_vac_list:list[CitiesVacancy]) -> None:
        self.city_vac_list = city_vac_list
        self.vac_num = len(self.city_vac_list)
        self.cities_vac_dict = {}
        self.cities_salary_dict = {}
        self.__make_cites_dicts()

    def __get_middle_cities_salary(self, city):
        if self.cities_vac_dict[city] == 0:
            return 0
        return self.cities_salary_dict[city] / self.cities_vac_dict[city]

    def __make_cites_dicts(self):
        for vacancy in self.city_vac_list:
            if vacancy.city_name not in self.cities_vac_dict:
                self.cities_vac_dict[vacancy.city_name] = 1
            else:
                self.cities_vac_dict[vacancy.city_name] += 1

            if vacancy.city_name not in self.cities_salary_dict:
                self.cities_salary_dict[vacancy.city_name] = vacancy.vac_salary.rur_middle_salary
            else:
                self.cities_salary_dict[vacancy.city_name] += vacancy.vac_salary.rur_middle_salary
            

        self.cities_vac_dict = {
            city: self.cities_vac_dict[city]
            for city in sorted(
            [perc_city for perc_city in self.cities_vac_dict.keys() if 
            self.cities_vac_dict[perc_city]>= int(0.01 * self.vac_num)],
            key=lambda perc_city: self.cities_vac_dict[perc_city],
            reverse=True)}

        self.cities_salary_dict = {
            city: self.__get_middle_cities_salary(city)
            for city in sorted(
                [vacs_city for vacs_city in self.cities_vac_dict.keys()],
                key=lambda city_nmae: self.cities_salary_dict[city_nmae],
                reverse=True)
        }
        self.cities_vac_dict = {
            city: self.cities_vac_dict[city]/self.vac_num for city in self.cities_vac_dict.keys()
        }
class YearStat:
    '''
    представляет статистику за год по 4-м катигориям
    вакансий за год
    вакансий по выбранной профессии за год
    ср. зарплата за год
    ср. зарплата за год для выбранной профессии
    '''
    def __init__(self, years_vacancies:list[YearsVacancy], prof_name) -> None:
        self.years_vacancies = years_vacancies
        self.prof_name = prof_name

        self.year = years_vacancies[0].published_year
        self.vac_number = 0
        self.vac_prof_number = 0
        self.middle_salary = 0
        self.middle_prof_salary = 0
        self.__make_year_stat()

    def __make_year_stat(self):
        for vacancy in self.years_vacancies:
            self.vac_number += 1
            self.middle_salary += vacancy.vac_salary.rur_middle_salary

            if self.prof_name in vacancy.vac_name:
                self.vac_prof_number += 1
                self.middle_prof_salary += vacancy.vac_salary.rur_middle_salary

        if self.vac_number != 0:
            self.middle_salary /= self.vac_number
        if self.vac_prof_number != 0:
            self.middle_prof_salary /= self.vac_prof_number

class SmartDataset:
    '''
    датасет состоит из 2-х основных частей информации, нужной для построения статистики по годам
    и для построения статистики по городам
    year_set -- список из объектов YearStat
    cities_set -- список из объектов CitiesVacancy
    (информация из строки csv-файла идет в 2 объекта и, затем, в 2 списка)
    '''
    def __init__(self, path_to_file) -> None:
        self.path = path_to_file
        self.year_set, self.cities_set = self.__get_sets()
                                
    def __get_year(self, str_date_time):
        norm_soformat = str_date_time[0:-2]+':'+str_date_time[-1]+str_date_time[-2]
        return datetime.datetime.fromisoformat(norm_soformat)

    def  __get_sets(self):
        '''
        возвращает два списка
        years_vacancies -- будет дальше использоваться в процессе для сбора статистики по годам
        cities_vacancies -- уйдет в years_data_proxy_list для статистики по городам
        '''
        years_vacancies = []
        cities_vacancies = []
        with open(self.path, encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            head = reader.__next__()
            head_len = len(head)
            num = 0
            for vacancy in reader:
                num+=1
                if len(vacancy) == head_len and '' not in vacancy:
                    vac_name = vacancy[head.index('name')]
                    salary_from = int(float(vacancy[head.index('salary_from')]))
                    salary_to = int(float(vacancy[head.index('salary_to')]))
                    salary_currency = vacancy[head.index('salary_currency')]
                    city_name = vacancy[head.index('area_name')]
                    vac_salary = Salary(salary_from, salary_to, salary_currency)
                    published_year = self.__get_year(vacancy[head.index('published_at')]).year
                    years_vacancies.append(YearsVacancy(vac_name, vac_salary, published_year))
                    cities_vacancies.append(CitiesVacancy(city_name, vac_salary))
        return years_vacancies, cities_vacancies

def get_year_stat(path_to_file: Path,prof_name:str, cities_data_proxy_list: list, condition: mp.Condition,
    years_vac_dict,
    yaers_vac_prof_dict,
    years_salary_dict,
    years_salary_prof_dict,
    number_of_files: int):
    '''
    основная функция сбора статистик за один год
    результаты идут в прокси-словари, кторые потом будут показаны в основном процессе
    '''
    datasset = SmartDataset(path_to_file)
    cities_data_proxy_list.append(datasset.cities_set)
    if len(cities_data_proxy_list) == number_of_files:
        #если собраны вакансии из всех файлов, основной процесс уведомляется, чтобы начать собирать статитстику по городам
        with condition:
            condition.notify()
    stat = YearStat(datasset.year_set, prof_name)
    years_vac_dict[stat.year] = stat.vac_number
    yaers_vac_prof_dict[stat.year] = stat.vac_prof_number
    years_salary_dict[stat.year] = stat.middle_salary
    years_salary_prof_dict[stat.year] = stat.middle_prof_salary
def sort_yeat_dict(year_dict: dict):
    return {
        year:year_dict[year] for year in sorted(list(year_dict.keys()))}
if __name__ == '__main__':
    '''
    статистика собирается в несколько процессов,
    каждый процесс обрабатывает свой файл, также в главном процессе(этом)
    собирается статистика по городам.
    используется years_data_proxy_list -- список, в который каждый процес копирует данные, которые он собрал,
    таким образом, когда длина списка равна количеству файлов, в нем есть все данные из всех .csv файлов
    '''
    connect = InputConnect()
    this_dir = Path(__file__).resolve().parent

    years_dir_path = Path.joinpath(this_dir, connect.file_name)
    prof_name = connect.profession

    years_dir = Path(years_dir_path)
    paths_to_files = [fullpath for fullpath in years_dir.iterdir()]
    condition = mp.Condition()
    number_of_files = len(paths_to_files)
    manager = mp.Manager()

    cities_data_proxy_list = manager.list()#список в который помещаются списики из годовых датасетов

    years_vac_dict = manager.dict()
    yaers_vac_prof_dict = manager.dict()
    years_salary_dict = manager.dict()
    years_salary_prof_dict = manager.dict()

    proc_list = [
        mp.Process(
        target=get_year_stat, 
        args=(
            paths_to_files[i],
            prof_name,
            cities_data_proxy_list, 
            condition,
            years_vac_dict,
            yaers_vac_prof_dict,
            years_salary_dict,
            years_salary_prof_dict,
            number_of_files)) 
        for i in range(number_of_files)
        ]
    
    for i in range(number_of_files):
        proc_list[i].start()
    
    with condition:
        condition.wait()
    
    if len(cities_data_proxy_list) == number_of_files:
        sities_set = [cities_vacancy 
        for years_list in cities_data_proxy_list 
        for cities_vacancy in years_list]#распаковка списка из данных о вакансиях по годам в один список для сбора статистики по городам
        
        cities_stat = CitiesStat(sities_set)

    for i in range(number_of_files):
        proc_list[i].join()

    print('Динамика уровня зарплат по годам:',sort_yeat_dict(years_salary_dict))
    print('Динамика количества вакансий по годам:',sort_yeat_dict(years_vac_dict))
    print('Динамика уровня зарплат по годам для выбранной профессии:',sort_yeat_dict(years_salary_prof_dict))
    print('Динамика количества вакансий по годам для выбранной профессии:',sort_yeat_dict(yaers_vac_prof_dict))
    print('Уровень зарплат по городам (в порядке убывания):',cities_stat.cities_salary_dict)
    print('Доля вакансий по городам (в порядке убывания):',cities_stat.cities_vac_dict)