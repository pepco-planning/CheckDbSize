import os
import pandas as pd
import datetime
import numpy as np
import re


targetValue_fileName_mail = "DBSize_mail.csv"
targetValue_fileName = "DBSize.csv"
targetValue_filePath = r"E:\badamczyk"
targetValue_filePathWithName = targetValue_filePath + "\\" + targetValue_fileName
targetValue_filePathWithName_mail = targetValue_filePath + "\\" + targetValue_fileName_mail



if os.path.isfile(targetValue_filePathWithName):
    os.remove(targetValue_filePathWithName)
resultTable = pd.DataFrame(columns=['TABLESPACE_NAME',
                                     'FREE_SPACE_MB',
                                     'MAX_SZ_MB',
                                     'PCT_FULL',
                                     'week',
                                     'Difference',
                                     'Cover'])


resultTable.to_csv(targetValue_filePathWithName, mode="a+", index=False, header=True)


data = '2020-06-25'
## poczatek tworzenia pliku DBSize.csv

if not (re.match(r"[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]", data)):
    print("Error! Podany format jest niepoprawny")
    opens()
check = datetime.date(int(data[0:4]), int(data[5:7]), int(data[8:10]))
data = check
dzis = datetime.datetime.now()
d = dzis.timetuple()
dz = datetime.date(d.tm_year, d.tm_mon, d.tm_mday)

## kompresja danych do jendego pliku
while data <= dz:
    date = str(data)
    dat = date
    date = date + "Second.csv"
    targetValue_fileNamee = date
    targetValue_filePathh = r"E:\manowak\DBSize"
    targetValue_filePathhWithName = targetValue_filePathh + "\\" + targetValue_fileNamee
    dzt = data.timetuple()


    if os.path.exists(targetValue_filePathhWithName)and dzt.tm_wday == 1 :
        df = pd.read_csv(targetValue_filePathhWithName, delim_whitespace=True).loc[1:]
        #df.dropna(subset='MAX_SZ_MB')
        df.drop(len(df), axis=0, inplace=True)
        df.FREE_SPACE_MB = df.FREE_SPACE_MB.astype('int32')
        df["week"] = dat
        df["Difference"] = 0
        df["Cover (weeks)"] = round(df.FREE_SPACE_MB / 4700, 2)
        df.drop(['CUR_USE_MB', 'CUR_PCT_FULL', 'CUR_SZ_MB'], axis=1, inplace=True)
        # ['TABLESPACE_NAME','FREE_SPACE_MB', 'MAX_SZ_MB', 'PCT_FULL'] te tabele zostaja
        colsToRound = ["Difference", "Cover (weeks)"]
        df[colsToRound] = df[colsToRound].round(1).astype(float)

        #dodawanie do pliku
        df[['TABLESPACE_NAME',
            'FREE_SPACE_MB',
            'MAX_SZ_MB',
            'PCT_FULL',
            'week',
            'Difference',
            'Cover (weeks)']].to_csv(targetValue_filePathWithName, mode="a", index=False, header=False)
        check = datetime.date(int(dat[0:4]), int(dat[5:7]), int(dat[8:10]))

        check = check + datetime.timedelta(days=1)
        data = check
    else:
        check = datetime.date(int(dat[0:4]), int(dat[5:7]), int(dat[8:10]))
        check = check + datetime.timedelta(days=1)
        data = check
## nadpisanie wartosci Difference oraz Cover
if os.path.exists(targetValue_filePathWithName):
    df = pd.read_csv(targetValue_filePathWithName, sep=',')

    wartosci_unikalne = np.unique(df['TABLESPACE_NAME'])

    lista_index = []
    lista_wartosc = []

    # pętla na skakanie po wierszach unikalnych
    for k in wartosci_unikalne:
        dk = df[df['TABLESPACE_NAME'] == k]

        # pętla do obliczenia różnicy
        i = 0
        while i in range(0, len(dk), 1):
            if i == len(dk) - 1:
                lista_index.append(int(dk.index[i]))
                lista_wartosc.append(int(0))
                i += 1
            else:
                zmienna_1 = int(dk.loc[dk.index[i], ['FREE_SPACE_MB']])
                zmienna_2 = int(dk.loc[dk.index[i + 1], ['FREE_SPACE_MB']])
                roznica_pomiedzy = int(zmienna_1 - zmienna_2)
                if roznica_pomiedzy < 0:
                    lista_index.append(int(dk.index[i]))
                    lista_wartosc.append(int(0))
                    # if abs(roznica_pomiedzy) > 20000 and abs(roznica_pomiedzy) < 32000:
                    # zmienna_1 += 32000
                    # roznica_pomiedzy = zmienna_1 - zmienna_2
                    # lista_index.append(dk.index[i])
                    # lista_wartosc.append(int(roznica_pomiedzy))
                    # i += 1
                    # if abs(roznica_pomiedzy) > 84000 and abs(roznica_pomiedzy) < 96000:
                    # zmienna_1 += 96000
                    # roznica_pomiedzy = zmienna_1 - zmienna_2
                    # lista_index.append(dk.index[i])
                    # lista_wartosc.append(int(roznica_pomiedzy))
                    # i += 1
                    # else:
                    # else:
                    # zmienna_1+=64000
                    # roznica_pomiedzy = zmienna_1 - zmienna_2
                    # slownik[dk.index,roznica] = [dk.index[i],roznica_pomiedzy]
                    i += 1
                else:
                    lista_index.append(int(dk.index[i]))
                    lista_wartosc.append(int(roznica_pomiedzy))
                    i += 1



    for i in range(0, len(lista_index)):
        df.loc[lista_index[i], 'Difference'] = lista_wartosc[i]

    tablename = df[['TABLESPACE_NAME', 'Difference']]
    tablename = tablename.groupby(['TABLESPACE_NAME'], as_index=False).mean()
    tablename.rename(columns={'Difference': 'AverageDiff'}, inplace=True)
    tablespace_list = list(df.TABLESPACE_NAME.unique())

    df = pd.merge(df, tablename, on='TABLESPACE_NAME', how='inner')
    df['Cover'] = np.where(df.AverageDiff > 0, round(df.FREE_SPACE_MB / df.AverageDiff,0), 'brak zmian')


if os.path.exists(targetValue_filePathWithName):
    os.remove(targetValue_filePathWithName)
resultTable = pd.DataFrame(columns=['TABLESPACE_NAME',
                                     'FREE_SPACE_MB',
                                     'MAX_SZ_MB',
                                     'PCT_FULL',
                                     'week',
                                     'Difference',
                                     'Cover',
                                    'AverageDiff'])


resultTable.to_csv(targetValue_filePathWithName, mode="a+", index=False, header=True)

df[['TABLESPACE_NAME',
            'FREE_SPACE_MB',
            'MAX_SZ_MB',
            'PCT_FULL',
            'week',
            'Difference',
            'Cover',
            'AverageDiff']].to_csv(targetValue_filePathWithName, mode="a", index=False, header=False)
## koniec nadpisywania wartości Difference oraz cover
## koniec tworzenia pliku DBSize.csv

## Poczatek tworzenia DBSize_mail.csv
if os.path.exists(targetValue_filePathWithName):
    df = pd.read_csv(targetValue_filePathWithName, sep=',')

    tabela_do_maila = df[df.week== str(dz)]
    tabela_do_maila = tabela_do_maila[tabela_do_maila.Cover != 'brak zmian']
    tabela_do_maila.drop(['Difference', 'MAX_SZ_MB','AverageDiff',], axis=1, inplace=True)
    tabela_do_maila['Cover'] = tabela_do_maila.Cover.astype('float64')
    tabela_do_maila.sort_values('Cover', inplace=True)




if os.path.isfile(targetValue_filePathWithName_mail):
    os.remove(targetValue_filePathWithName_mail)
resultTablee = pd.DataFrame(columns=['TABLESPACE_NAME','FREE_SPACE_MB','PCT_FULL','week','Cover'])


resultTablee.to_csv(targetValue_filePathWithName_mail, mode="a+", index=False, header=True)
tabela_do_maila[['TABLESPACE_NAME','FREE_SPACE_MB','PCT_FULL','week','Cover']].to_csv(targetValue_filePathWithName_mail, mode="a", index=False, header=False)

## koniec tworzenia DBSize_mail.csv



