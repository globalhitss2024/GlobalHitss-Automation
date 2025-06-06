import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos
DATABASE = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'database_prod': os.getenv('DB_PROD_NAME')
}

# Configuración de rutas
PATHS = {
    'base': os.getenv('BASE_PATH'),
    'log': os.getenv('LOG_PATH'),
    'conciliaciones_file': os.getenv('CONCILIACIONES_FILE_PATH')
}

def get_db_url(production=True):
    """Retorna la URL de conexión a la base de datos"""
    db = DATABASE
    db_name = db['database_prod'] if production else db['database']
    return f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db_name}"

class Config:
    """Configuración centralizada"""
    
    # Base Database
    DB = DATABASE

    # Process Control
    PROCESS = {
        'batch_size': int(os.getenv('BATCH_SIZE', 1000)),
        'timeout': int(os.getenv('TIMEOUT', 300)),
        'max_retries': int(os.getenv('MAX_RETRIES', 3))
    }
    
    # File Sources
    SOURCES = {
        'legalizadas': {
            'file1': os.getenv('LEGALIZED_FILE_1'),
            'file2': os.getenv('LEGALIZED_FILE_2'),
            'sheet1': os.getenv('LEGALIZED_SHEET_1')
        },
        'cloud': {
            'file': os.getenv('CLOUD_FILE'),
            'sheet': os.getenv('CLOUD_SHEET')
        },
        'maestro_codigos': {
            'file': os.getenv('MASTER_CODES_FILE'),
            'sheet': os.getenv('MASTER_CODES_SHEET')
        }
    }

    # Logging
    LOG = {
        'level': os.getenv('LOG_LEVEL', 'INFO'),
        'format': "%(asctime)s - %(levelname)s - %(message)s",
        'directory': os.getenv('LOG_PATH')
    }

    # Denodo connection
    DENODO = {
        'user': os.getenv('DENODO_USER'),
        'password': os.getenv('DENODO_PASSWORD')
    }

    # SQL Server connection
    SQLSERVER = {
        'user': os.getenv('SQLSERVER_USER'),
        'password': os.getenv('SQLSERVER_PASSWORD'),
        'host': os.getenv('SQLSERVER_HOST'),
        'database': os.getenv('SQLSERVER_DB'),
        'database_temp': os.getenv('SQLSERVER_DB_TEMP')
    }

    # Additional paths
    PATHS.update({
        'planta_comercial': os.getenv('PLANTA_COMERCIAL_PATH'),
        'valor_agregado': os.getenv('VALOR_AGREGADO_PATH'),
        'red_fttx': os.getenv('RED_FTTX_PATH'),
        'transfers': os.getenv('TRANSFERS_PATH'),
        'ngn': os.getenv('NGN_PATH')
    })

    # Azure configuration
    AZURE = {
        'tenant_id': os.getenv('AZURE_TENANT_ID'),
        'client_id': os.getenv('AZURE_CLIENT_ID'),
        'client_secret': os.getenv('AZURE_CLIENT_SECRET')
    }

    @staticmethod
    def get_sqlserver_url():
        """Genera URL de conexión para SQL Server"""
        sql = Config.SQLSERVER
        return f"mssql+pyodbc://{sql['user']}:{sql['password']}@{sql['host']}/{sql['database']}?driver=ODBC+Driver+17+for+SQL+Server"

    @staticmethod
    def get_source_path(source_type):
        """Retorna ruta completa de fuente"""
        return os.path.join(
            os.getenv('SOURCES_PATH'), 
            f"base_{source_type}"
        )

    @staticmethod 
    def get_log_path(script_name):
        """Retorna ruta de log para un script"""
        return os.path.join(
            Config.LOG['directory'],
            f"{script_name}.log"
        )
