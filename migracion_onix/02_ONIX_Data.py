#############################################################################################################
#                                  @AUTOR: LORENA HERNANDEZ  
#                                  @ACTUALIZACIÓN: FERNANDA ZAMBRANO           	                        	#
#                                  @PROCESO: MIGRACION DE INFORMACION ONIX AL SERVIDOR POSTGRES             #                                                     									                        #
#                                  @DESCRIPCIÓN: SE TOMA LA INFORMACIPON DEL CLIENTE BÁSICA Y NECESARIA     #
#                                  Y SE MIGRA AL SERVIDOR                                                   #
#############################################################################################################


from Conect import *  # Importa todas las funciones del módulo 'Conect'
from Credenciales import *  # Importa todas las funciones del módulo 'Credenciales'
from sqlalchemy import inspect, text  #Permite la conexión con BD y la ejecución de consultas SQL

################################# SQL SERVER #####################################

# Establece las variables de conexión para SQL Server utilizando las credenciales importadas

ipsql= ipsql
portsql= portsql
usersql=usersql
passwordsql= passwordsql
bbddsql= bbddsql

# Crear la conexión con SQL Server

sqlserver_conn = f'mssql+pymssql://{usersql}:{quote(passwordsql)}@{ipsql}:{portsql}/{bbddsql}'
sql = create_engine(sqlserver_conn)

## Consulta SQL para obtener datos de la base de datos SQL Server
consulta_sql = """ 

SELECT 
     a.iCompanyId,
    -- a.iSiteId, ## TODO DICE 1
    -- a.chLanguageCode, ESP
    a.vchAssignedId AS NIT,
    a.vchCompanyName AS RAZON_SOCIAL,
    -- a.vchAddress1 AS DIRECCION,
    -- a.vchAddress2 ZONA,
    -- a.vchAddress3,
    a.vchCity AS CIUDAD,
    a.vchRegionCode AS REGION,
    a.vchCountryCode AS PAIS,
    a.vchPhoneNumber AS telefono,
    a.vchEmailAddress AS correo,
    -- a.vchPostCode,
    -- a.vchPhoneNumber,
    -- a.vchEmailAddress,
    -- a.iCompanyTypeCode,
    -- a.iCompanySubTypeCode,
    -- a.iFamilyId, --## dice Teléfono
     z.vchFamilyName as GRUPO_ECONOMICO,
    -- a.iParentId, ## viene todo en null
    -- a.iPrimaryContactId, #Dato de tipificaion contactabilidad
    -- a.vchContactFirstName, #Dato de tipificaion contactabilidad
    -- a.vchContactLastName, #Dato de tipificaion contactabilidad
    -- a.iDivisionCode, ##se encuentra en ceros
    -- a.iSICCode, SECTOR
    b.vchParameterDesc AS SECTOR,
   	-- a.iMarketSector, SEGMENTO
    c.vchParameterDesc AS SEGMENTO,
    -- a.vchTaxId, ##registros vacios
    -- a.vchDunnsNumber, ##registros vacios
    -- a.iPhoneTypeId,#Dato de contactabilidad Telefono contacto etc
    -- a.iAddressTypeId, 
    d.vchParameterDesc AS AddressType,
    -- a.iSourceId, ##especifica si la informacion se estrae por camara de comercio, otras fuentes, consultor etc
    -- a.iStatusId,	paises extranjeros 
    -- a.bValidAddress,
    -- a.iAccessCode, ##O.T. En todos los campos
    -- a.bPrivate, ## dice lo mismo o.t.
    -- a.vchUser1,## es el mismo nit con digito de verificacion
    -- a.vchUser2, algunas ubicaciones ej zona franca 
    a.vchUser3 AS ACTIVIDAD_ECO, 
    -- a.vchUser4,
    -- a.vchUser5,
    -- a.vchUser6,
    -- a.vchUser7,
	-- a.vchUser8,
    -- a.vchUser9, -- ##VALIDAR CON WIL
    -- a.vchUser10, -- ##VALIDAR CON WIL (medium, large, sin clasificar, small potencial)
    -- a.chInsertBy, -- TRAE UN USUARIO DE RED
	FORMAT(a.dtInsertDate, 'yyyy-MM-dd HH:mm:ss' ) AS INSERT_DATE, 
    -- a.chUpdateBy, usauerio ingreso onix
    format(a.dtUpdateDate, 'yyyy-MM-dd HH:mm:ss' ) AS DATE_UPDATE,
    a.tiRecordStatus
    -- a.dtModifiedDate
    
FROM onyx.dbo.Company AS a

LEFT JOIN (
    SELECT aa.iCompanyId,
           aa.iSICCode,
           bb.vchParameterDesc
    FROM onyx.dbo.Company aa
    LEFT JOIN onyx.dbo.reference_parameters bb ON aa.iSICCode = bb.iParameterId
    
) b ON A.iCompanyId = b.iCompanyId


LEFT JOIN (
    SELECT aa.iCompanyId,
           aa.iMarketSector,
           bb.vchParameterDesc
    FROM onyx.dbo.Company aa
    LEFT JOIN onyx.dbo.reference_parameters bb ON aa.iMarketSector = bb.iParameterId
  ) c ON A.iCompanyId = c.iCompanyId    

  


LEFT JOIN (
    SELECT aa.iCompanyId,
           aa.iAddressTypeId,
           bb.vchParameterDesc
    FROM onyx.dbo.Company aa
    LEFT JOIN onyx.dbo.reference_parameters bb ON aa.iAddressTypeId = bb.iParameterId
    
) d ON A.iCompanyId = d.iCompanyId


LEFT JOIN (
    SELECT aa.iCompanyId,
    	   aa.iFamilyId,
           bb.vchFamilyName
    FROM onyx.dbo.Company aa
    LEFT JOIN onyx.dbo.vCompanyFamily bb ON aa.iFamilyId = bb.iFamilyId
    
) z ON A.iCompanyId = z.iCompanyId

WHERE a.tiRecordStatus = '1'

"""

# Ejecuta la consulta SQL y carga los resultados en un DataFrame de Pandas
df = pd.read_sql(consulta_sql, sql)

# Ahora df contiene los datos de la tabla
print(df)


################################# POSTGRES #####################################

# Configura las credenciales para la conexión a PostgreSQL

ip= ip
port= port  # El puerto predeterminado de PostgreSQL es 5432
user= user
password= password
bbdd= bbdd

# Crear la conexión
postgres_conn_str = f'postgresql://{user}:{password}@{ip}:{port}/{bbdd}'
engine = create_engine(postgres_conn_str)

# Verificar si la tabla existe y la trunca si es necesario
inspector = inspect(engine)
try:
    with engine.connect() as connection:
        if inspector.has_table("tbl_Company"):
            print("La tabla 'tbl_Company' ya existe. Se procederá a truncarla.")
            connection.execute(text('TRUNCATE TABLE "tbl_Company"'))
            connection.commit()
        else:
            print("La tabla 'tbl_Company' no existe. Se creará al cargar los datos.")

    # Cargar los datos en la tabla. 'append' la creará si no existe.
    to_sql(df, "tbl_Company", engine, 'append', False, 10000)
    print("Datos cargados exitosamente en 'tbl_Company'.")

except Exception as e:
    print(f"Ocurrió un error durante la carga a PostgreSQL: {e}")