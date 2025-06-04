# Procesamiento de Altas Segmento Empresas

## Descripción
Este módulo gestiona el procesamiento de datos de altas para el segmento empresarial, incluyendo la captura, transformación y carga de información relacionada con nuevas altas de servicios.

### Scripts Principales

#### cargue_datos_crudos_altas_segmento_empresas.py
- Procesa datos de altas empresariales
- Realiza transformaciones y validaciones de datos
- Gestiona la lógica de carga incremental
- Implementa controles de calidad de datos
- Maneja excepciones y logging

### Funcionalidades Principales
- Extracción desde múltiples fuentes
- Validación y limpieza de datos
- Transformaciones específicas del negocio
- Carga incremental a base de datos
- Sistema de logging detallado
- Manejo de errores y excepciones
- Control de duplicados
- Seguimiento de estados

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_altas_empresas
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados de procesamiento
- Contadores de registros
- Códigos de error
- Logs de ejecución

### Dependencias
- pandas
- sqlalchemy
- psycopg2
- logging
- datetime

### Flujo de Proceso
1. Lectura de fuentes
2. Validación de datos
3. Transformaciones
4. Carga en base de datos
5. Registro de resultados
6. Generación de logs

### Mantenimiento
- Actualización mensual de reglas de negocio
- Monitoreo diario de ejecución
- Gestión de errores
- Optimización de rendimiento
