# Procesamiento de Denodo Instaladas RR

## Descripción
Este módulo gestiona el procesamiento de datos de órdenes instaladas de RR desde Denodo.

### Scripts Principales

#### cargue_datos_crudos_instaladas_rr.py
- Procesa datos de órdenes instaladas RR
- Implementa transformaciones específicas
- Gestiona carga en base de datos
- Maneja validaciones y controles

### Funcionalidades Principales
- Extracción desde Denodo
- Validación de datos
- Transformaciones específicas
- Carga incremental
- Sistema de logging
- Control de errores

### Tablas Relacionadas
- fuentes_cruda.tb_datos_crudos_denodo_instaladas_rr
- control_procesamiento.tb_resumen_cargue
- control_procesamiento.tb_errores_cargue

### Variables de Control
- ID ejecución
- Estados
- Contadores
- Errores
- Logs
