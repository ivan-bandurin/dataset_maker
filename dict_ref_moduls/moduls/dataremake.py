import pandas
from numpy import nan

# Функция удаления первых лишних строк для любого датасета, загруженного из excel файла
# ВАЖНО! Функция работает для excel-файлов, столбцы которых - это годы 
# На вход подается: датасет; кусок слова, с которого начинается первая строка данных
# На выходе получается: датасет, очищенный от первых строк и в качестве столбцов получающий годы
def cleaning_from_first_rows(data, word):
    name = data.name
    index_target = 0
    column_target = 0

# Блок присвоения таблице наименований столбцов по годам
    
    # Цикл проходит все столбцы в таблице и находит столбец, в котором содержатся регионы    
#     for col in data.columns:        
#         if any(word in x for x in [str(x) for x in list(data[col])]):
#             column_target = col
    for col in data.columns:
        for i in range(len(data[col])):
            if word in str(data.loc[i,col]):
                i_first_object_name = i
                column_target = col
    # years - список, содержащий в себе все годы с 1990 по 2021
    years = [str(3000)]
    
    # цикл проходит все строки и все столбцы начиная со следующего, после того, в котором содержатся регионы
    
    for i in range(i_first_object_name):
        for col in (data.columns[(data.columns.get_loc(column_target)+1):]):
            
            if any(x in str(data.loc[i, col]) for x in years):
                index_target = i  

    data.at[index_target, column_target] = 'object_name'    
    data.columns = data.iloc[index_target]
    
# Блок удаления лишних первых строк

    for col in (data.columns):
        try:
            if any(word in x for x in [str(x) for x in list(data[col])]):
                column_target = col
        except:
            continue
    
    for i in range(len(data)):        
        if word in str(data.loc[i, column_target]):
            index_target = i  
    
    data = data.drop(index=list(range(index_target))).reset_index(drop=True)
    
# Чистка оставшихся пропусков в столбце с регионами

    data = data.dropna(subset=['object_name']).reset_index(drop=True)
    data.name = name
    return(data)

def comments_def(data):

    name = data.name
    indicator = 0
    for element in data['object_name'].unique():
        if element[0] in [str(x) for x in list(range(10))]:
            indicator += 1
    if indicator == 0:
        return data
    
    else:
        
        commentary_data = []
        bad_element = []

        for element in data['object_name'].unique():

            if element[0] in [str(x) for x in list(range(10))]:
                bad_element.append(element)
                n = element[0]
                element = element.replace(element[0],'')
                commentary_data.append([data.name, n, element])
        commentary = pandas.DataFrame(data = commentary_data, columns = ['dataset','commentary_number','commentary_text'])
        regions_data = []
        for i in range(len(data)):
            if data.loc[i,'object_name'][len(data.loc[i,'object_name'])-1] in [str(x) for x in list(range(10))]:
                n = data.loc[i,'object_name'][len(data.loc[i,'object_name'])-1]
                data.at[i,'object_name'] = data.loc[i,'object_name'].replace(data.loc[i,'object_name'][len(data.loc[i,'object_name'])-1],'')
                regions_data.append([n, data.loc[i,'object_name']])
        regions = pandas.DataFrame(data = regions_data, columns = ['commentary_number','region_name'])
        commentary = commentary.merge(regions, on = 'commentary_number')
        data = data[~data['object_name'].isin(bad_element)]   
        data.name = name
        return data, commentary

# Функция создания значений по годам по регионам
def make_column_def(data, column_name):
    data = data.set_index('object_name')
    data = data.stack()
    data = data.rename_axis([2, 1]).reset_index()
    data.columns = ['object_name','year',column_name]
    return data

# Функция определения кода ОКАТО каждого региона в списке ()
def okato_def(raw):
    dict_path = 'C:/Users/User/Desktop/Росстат/Проект_Датасеты/Датасеты/Демография/!Датасет_демография/!workspace/dict_ref_moduls/'
    data_okato = pandas.read_csv(dict_path + 'data_okato.csv', index_col = 'Unnamed: 0')
    
    try:
        

        test = str(raw['object_name'])

        if 'томская' in test.lower():        
            return data_okato.loc[data_okato.index[data_okato['marker'] == 'томская'].to_list()[0], 'okato']
        elif 'костромская' in test.lower():
            return data_okato.loc[data_okato.index[data_okato['marker'] == 'костромская'].to_list()[0], 'okato']
        elif 'ямало' in test.lower():
            return data_okato.loc[data_okato.index[data_okato['marker'] == 'ямало-ненецкий'].to_list()[0], 'okato']
        elif 'алтайский' in test.lower():
            return data_okato.loc[data_okato.index[data_okato['marker'] == 'алтайский'].to_list()[0], 'okato']

        else: 
            for j in range(len(data_okato)):
                if 'томская' in test.lower() or 'костромская' in test.lower() or 'ямало' in test.lower():
                    continue
                elif 'район' in test.lower():
                    return nan    
                elif any(x in test.lower() for x in ['кроме', 'без']) is True:
                    if data_okato.loc[j,'marker'] in test.lower() and 'кроме' in data_okato.loc[j,'okato_name']:
                        return data_okato.loc[j,'okato']
                elif any(x in test.lower() for x in ['кроме', 'без']) is False and 'кроме' not in data_okato.loc[j,'okato_name']:
                    if data_okato.loc[j,'marker'] in test.lower():
                        return data_okato.loc[j,'okato']
                
    except:
        return nan

def correct_name_def(raw):
    
    dict_path = 'C:/Users/User/Desktop/Росстат/Проект_Датасеты/Датасеты/Демография/!Датасет_демография/!workspace/dict_ref_moduls/'
    
    try:
        data_okato = pandas.read_csv(dict_path + 'data_okato.csv', index_col = 'Unnamed: 0')
        okato = raw['object_okato']
        try:
            correct_name = data_okato.loc[data_okato.index[data_okato['okato'] == okato].to_list()[0], 'okato_name']
            return correct_name 
        except:
            return raw['object_name']
    except:
        return raw['object_name']

# Функция постановки столбца ОКАТО около региона
def nice_look_def(data):
    new_columns = data.columns.to_list()
    new_columns.insert(1, new_columns[len(new_columns)-1])
    new_columns.pop(len(new_columns)-1)
    data = data.reindex(columns=new_columns) 
    return data

