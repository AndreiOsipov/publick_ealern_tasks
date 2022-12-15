import csv
from pathlib import Path
class CsvSeparator:
    """
    класс, который умеет разделять один csv файл на несколько других
    по по году публикации. Каждый новый файл имет вид [год].csv, например 2007.csv\n
    при этом будет создана новая папка years\n
    self.file_name -- имя файла, который нужно разделить(либо абсолютный путь, если файл в другой дирректории)\n
    self.head (list[string]) --  заголовок csv-файла\n
    self.this_dir(Path) -- абсолютный путь до дирректории,\n
    в которой находится скрипт
    self.csv_file_name_list -- список имен файлов, которые уже созданы\n
    """
    def __init__(self, file_name: str) -> None:
        self.head = []
        self.this_dir = Path(__file__).resolve().parent
        self.years_dir_path = Path.joinpath(self.this_dir, 'years')

        self.csv_file_name_list = []
        self.file_name = file_name

    def __generate_new_csv_file(self, data, path):
        '''
        создает новый файл, в дирректории years\n
        необходимая кодировка -- utf-8-sig
        '''
        with open(path, 'w', encoding='utf-8-sig') as file:
            file.writelines([str.join(',',self.head), str.join(',',data)])\

    def __update_csv_file(self, path, data):
        """
        добавляет новую строку в существующий csv файл
        """
        with open(path, 'a', encoding='utf-8-sig') as file: 
            file.write(str.join(',',data))

    def __get_years_abs_path(self, pub_date: str):
        '''
        создает абсолютный путь до файла в папке years,
        который, затем, будет создан
        '''
        year = (pub_date[0:4])+'.csv'
        return Path.joinpath(self.years_dir_path, year)
            
    def generate_csv_files(self):
        '''
        создает дирректорию years, после чего либо
        создаёт новый csv-файл, если год убликации, указанный в вакании встречается впервые,
        либо заносит строку в существущий csv-файл, для отслеживания созданных файлов используется
        поле self.csv_file_name_list
        '''
        Path.mkdir(self.years_dir_path)
        with open(self.file_name, encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            self.head = reader.__next__()
            pub_date_index = self.head.index('published_at')
            for line in reader:
                abs_apth  = self.__get_years_abs_path(line[pub_date_index]) 
                if abs_apth not in self.csv_file_name_list:
                    self.__generate_new_csv_file(line, abs_apth)
                    self.csv_file_name_list.append(abs_apth)
                else:
                    self.__update_csv_file(abs_apth, line)
def main():
    sep = CsvSeparator('vacancies_by_year.csv')
    sep.generate_csv_files()
    print('файл успешно разделен')
    
if __name__ == '__main__':
    main()

