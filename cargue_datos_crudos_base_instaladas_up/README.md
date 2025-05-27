# Procesamiento de Base Instaladas UP

## Descripción
Este módulo gestiona el procesamiento de datos de órdenes instaladas del sistema UP.

### Scripts Principales

#### cargue_datos_crudos_instaladas_up.py
- Procesa datos de instalaciones UP
- Implementa transformaciones específicas 
- Gestiona carga en base de datos
- Maneja validaciones y controles

### Funcionalidades Principales
- Importación desde CSV
- Validación de datos
- Transformaciones específicas
- Carga incremental 
- Sistema de logging
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_instaladas_up
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados
- Contadores
- Errores 
- Logs
