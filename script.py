#https://stackoverflow.com/questions/17972020/how-to-execute-raw-sql-in-flask-sqlalchemy-app
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd

def get_db_connection():
    '''
        Function to establish a connection to the database
    '''
    server = r'ERNESTOSLAP\SQLEXPRESS'  # Conexión a servidor SQL Server en mi localhost
    database = 'BDVentaPorDepartamento'  # Nombre de la BD proporcionada por el profesor.

    connection_string = f"mssql+pyodbc://@{server}/{database}?driver=SQL+Server&Trusted_Connection=yes" # Cadena de conexión a la base de datos

    try:
        engine = create_engine(connection_string) #Establezco conexión a la base de datos
        return engine

    except Exception as e:
        print(f"Ocurrió un error al conectarse a la base de datos: {e}")
        return None
    
def test_connection():
    '''
        Function to test the connection to the database
    '''
    engine = get_db_connection()

    query = 'SELECT top 10 * FROM VW_Ventas'
    df = pd.read_sql(query, engine)

    print("\n1. Primeras filas de la vista:")
    print(df.head())
    engine.dispose() # always a good practice to close the connection
    
    return

def get_top10_sales_countries():
    '''
        Function to get the top 10 countries with the most sales
    '''
    engine = get_db_connection()

    query = '''
       select 
            top 10 *
        from
        (select 
            
            distinct Pais as Pais,
            Sum(ImporteTotal) as TotVentad
        from VW_Ventas
        group by pais)as q1
        order by TotVentad desc
    '''
    df = pd.read_sql(query, engine)
    print("\n1. Total de ventas por país:")
    print(df)
    return

def get_sales_per_month():
    '''
        Function to get the total sales per month
    '''
    engine = get_db_connection()

    query = '''
        select 
            Ano,
            MesNumero,
            MesNombreAbrev,
            Sum(ImporteTotal) as TotVentad
        from VW_Ventas
        group by Ano,MesNumero,MesNombreAbrev
        order by Ano,MesNumero
    '''
    df = pd.read_sql(query, engine)
    df = df.drop(columns=['MesNumero'])
    df = df.rename(columns={'Ano':'Año','MesNombreAbrev':'Mes'})
    print("\n2. Total de ventas por mes:")
    print(df)
    return
  
def get_Top10_Sales_Products():
    '''
        Function to get the top 10 products with the most sales (in quantity)
    '''
    engine = get_db_connection()

    query = '''
        select 
            top 10 *
        from
            (select 
                
                distinct ProductoNumero as Producto,
                Sum(Cant) as UnidadesVendidas
            from VW_Ventas
            group by ProductoNumero)as q1
        order by UnidadesVendidas desc
    '''
    df = pd.read_sql(query, engine)
    print("\n3. Total de ventas por producto:")
    print(df)
    return

if __name__ == '__main__':
    '''
        Main function of the script
    '''
    #test_connection()
    get_top10_sales_countries()
    get_sales_per_month()
    get_Top10_Sales_Products()