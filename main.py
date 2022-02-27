import pandas as pd
import numpy as np 
import sqlalchemy
import sys

if len(sys.argv) <2:
    print("Ingresar la ruta del file a procesar")
else:
    file_procesar  = sys.argv[1]
    print(file_procesar)
    #Lectura de Datos:
    df_clientes = pd.read_csv(file_procesar, encoding='utf-8', sep=';')
    # Cambiar tipo de datos:
    df_clientes['fecha_vencimiento'] = pd.to_datetime(df_clientes['fecha_vencimiento'], format="%Y/%m/%d") 
    df_clientes['fecha_nacimiento'] = pd.to_datetime(df_clientes['fecha_nacimiento'], format="%Y/%m/%d") 
    # Cambiar Mayuscula:
    column_string =['fiscal_id','first_name','last_name','gender','direccion','estatus_contacto','correo']
    for i in column_string:
        df_clientes[i] = df_clientes[i].str.upper()
    
    #TABLA CLIENTES:
    #CREACION DE TABLA CLIENTES
    list_column_clientes = ['fiscal_id','first_name','last_name','gender','birth_date','age','age_group','due_date','delinquency','due_balance','address']
    clientes = pd.DataFrame(columns=list_column_clientes)
    #Poblar data:
    clientes[['fiscal_id','first_name','last_name','gender','birth_date', 'due_date','due_balance','address']] = df_clientes[['fiscal_id','first_name','last_name','gender','fecha_nacimiento','fecha_nacimiento','deuda','direccion']]
    #poblamos campo age:
    clientes['age'] = pd.to_datetime("today").year - df_clientes['fecha_nacimiento'].dt.year
    #poblar campo: age_group
    clientes['age_group']=clientes['age']
    clientes.loc[(clientes['age_group'] >=0) & (clientes['age_group'] <=20), 'age_group'] = 1
    clientes.loc[(clientes['age_group'] >=21) & (clientes['age_group'] <=30), 'age_group'] = 2
    clientes.loc[(clientes['age_group'] >=31) & (clientes['age_group'] <=40), 'age_group'] = 3
    clientes.loc[(clientes['age_group'] >=41) & (clientes['age_group'] <=50), 'age_group'] = 4
    clientes.loc[(clientes['age_group'] >=51) & (clientes['age_group'] <=60), 'age_group'] = 5
    clientes.loc[clientes['age_group'] >=61, 'age_group'] = 6
    #poblar campo: delinquency:
    clientes['delinquency'] = pd.to_datetime("today").day-df_clientes['fecha_vencimiento'].dt.day


    #TABLA EMAIL
    #creacion tabla clientes
    list_column_emails = ['fiscal_id','email','status','priority']
    emails = pd.DataFrame(columns=list_column_emails)
    emails[['fiscal_id','email','status','priority']]=df_clientes[['fiscal_id','correo','estatus_contacto','prioridad']]
    #Poner en 0 los valores nulos
    emails[['priority']] = emails[['priority']].fillna(0)
    #cambiamos tipo de dato a INT:
    emails['priority'] = emails['priority'].astype('int64')


    #TABLA PHONE
    #creacion tabla clientes
    list_column_phones = ['fiscal_id','phone','status','priority']
    phones = pd.DataFrame(columns=list_column_phones)
    phones[list_column_phones] = df_clientes[['fiscal_id','telefono','estatus_contacto','prioridad']]
    phones['phone']= phones['phone'].astype('str')
    #Poner en 0 los valores nulos
    phones[['priority']] = phones[['priority']].fillna(0)
    #cambiamos tipo de dato a INT:
    phones['priority'] = phones['priority'].astype('int64')

    #EXPORTAR:
    clientes.to_excel('clientes.xlsx', index= False)
    emails.to_excel('emails.xlsx',index= False)
    phones.to_excel('phones.xlsx',index= False)

    #CARGAR BASE DATOS:
    db_clientes = sqlalchemy.create_engine('sqlite:///C:\\Users\\milagros\\Documents\\python_test\\database.db3\\clientes.db')
    clientes.to_sql('clientes', db_clientes, if_exists="replace")
    db_emails = sqlalchemy.create_engine('sqlite:///C:\\Users\\milagros\\Documents\\python_test\\database.db3\\emails.db')
    emails.to_sql('emails', db_emails, if_exists="replace")
    db_phones = sqlalchemy.create_engine('sqlite:///C:\\Users\\milagros\\Documents\\python_test\\database.db3\\phones.db')
    phones.to_sql('phones', db_phones, if_exists="replace")