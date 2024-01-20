import sqlite3
import pandas as pd

con = sqlite3.connect("dorm.sqlite")
f_damp = open('dorm.db', 'r', encoding='utf-8-sig')
damp = f_damp.read()
f_damp.close()
con.executescript(damp)

con.commit()
cursor = con.cursor()

print('Запросы на выборку:')

df = pd.read_sql('''
    SELECT Комнаты.НомерКомнаты, Рабочие.Фамилия, Рабочие.Имя, СпециальностиРабочих.Специальность
    FROM Комнаты
    JOIN Рабочие ON Комнаты.СпециальностьИд = Рабочие.СпециальностьИд
    JOIN СпециальностиРабочих ON СпециальностиРабочих.СпециальностьИд = Рабочие.СпециальностьИд
    WHERE Комнаты.НомерОбщежития = 1
    ORDER BY Комнаты.НомерКомнаты ASC;
''', con)
print(df)
print('\n')

df = pd.read_sql('''
    SELECT ГрафикРабочих.*, Рабочие.Фамилия, Рабочие.Имя
    FROM ГрафикРабочих
    JOIN Рабочие ON ГрафикРабочих.РабочийИд = Рабочие.РабочийИд
    WHERE ГрафикРабочих.ДатаНачалаРаботы >= '2024-02-01'
    ORDER BY ГрафикРабочих.ДатаНачалаРаботы ASC;
''', con)
print(df)
print('\n')

print('Запросы с группировкой:')

df = pd.read_sql('''
    SELECT СпециальностиРабочих.Специальность, COUNT(Рабочие.РабочийИд) AS КоличествоРаботников
    FROM Рабочие
    JOIN СпециальностиРабочих ON Рабочие.СпециальностьИд = СпециальностиРабочих.СпециальностьИд
    GROUP BY СпециальностиРабочих.Специальность
    ORDER BY КоличествоРаботников ASC;
''', con)
print(df)
print('\n')

df = pd.read_sql('''
    SELECT Комнаты.НомерОбщежития, AVG(Комнаты.КоличествоМест) AS СреднееКоличество
    FROM Комнаты
    GROUP BY Комнаты.НомерОбщежития;
''', con)
print(df)
print('\n')

print('Запросы с вложенными запросами или табличными выражениями:')

df = pd.read_sql('''
    SELECT *
    FROM Рабочие
    WHERE РабочийИд IN (
        SELECT ГрафикРабочих.РабочийИд
        FROM ГрафикРабочих
        JOIN Комнаты ON (ГрафикРабочих.НомерКомнаты = Комнаты.НомерКомнаты) AND (Комнаты.КоличествоМест = (
            SELECT MAX(Комнаты.КоличествоМест) FROM Комнаты))
);
''', con)
print(df)
print('\n')

df = pd.read_sql('''
    WITH КомнатыРабочиеInfo AS (
        SELECT НомерКомнаты, Рабочие.Фамилия, Рабочие.Имя
        FROM Комнаты
        JOIN Рабочие ON Комнаты.СпециальностьИд = Рабочие.СпециальностьИд
    )
    SELECT * FROM КомнатыРабочиеInfo;
''', con)
print(df)
print('\n')

print('Запросы корректировки данных:')

cursor.execute('''
    UPDATE Комнаты
    SET КоличествоМест = 5
    WHERE НомерКомнаты = 1102;
''')
con.commit()

df = pd.read_sql('''
    SELECT *
    FROM Комнаты;
''', con)
print(df)
print('\n')




cursor.execute('''
    DELETE FROM ГрафикРабочих
    WHERE РабочийИд = 1;
''')
con.commit()

df = pd.read_sql('''
    SELECT *
    FROM ГрафикРабочих;
''', con)
print(df)
print('\n')

con.close()