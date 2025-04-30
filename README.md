# Comandos Básicos de Git

Este documento contiene una recopilación de comandos básicos de Git para facilitar el control de versiones en tus proyectos.

## 🔧 Configuración Inicial
```bash
git config --global user.name "Tu Nombre"
git config --global user.email "tu.email@ejemplo.com"
```

## 📁 Inicializar Repositorio
```bash
git init
```

## 📄 Agregar Archivos al Staging Area
```bash
git add nombre_archivo         # Agrega un solo archivo
git add .                      # Agrega todos los archivos del directorio actual
```

## 💾 Crear un Commit
```bash
git commit -m "Mensaje descriptivo"
```

## 🔍 Ver Estado del Repositorio
```bash
git status
```

## 🔄 Ver Cambios Realizados
```bash
git diff                       # Cambios sin añadir al staging
git diff --staged              # Cambios añadidos al staging
```

## 📚 Ver Historial de Commits
```bash
git log
```

## 🔗 Conectar a Repositorio Remoto
```bash
git remote add origin https://github.com/usuario/repositorio.git
```

## 🚀 Subir Cambios al Repositorio Remoto
```bash
git push -u origin main        # Primer push
git push                       # Siguientes veces
```

## ⬇️ Clonar un Repositorio
```bash
git clone https://github.com/usuario/repositorio.git
```

## 🔄 Traer Cambios del Repositorio Remoto
```bash
git pull
```

## 🌿 Crear y Cambiar de Rama
```bash
git branch nombre_rama         # Crear rama
git checkout nombre_rama       # Cambiar de rama
git checkout -b nombre_rama    # Crear y cambiar en un solo paso
```

## 🧹 Eliminar Rama
```bash
git branch -d nombre_rama      # Eliminar rama local
```

---

✅ **Sugerencia**: Para mantener tus commits organizados, realiza commits pequeños y frecuentes con mensajes descriptivos.
