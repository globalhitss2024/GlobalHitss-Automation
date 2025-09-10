
def limpiar_nit(nit: str) -> str:
    """
    Limpia el NIT cuando cumple la condición:
    - Si empieza con 8 o 9
    - Y tiene longitud de 10 dígitos
    Entonces elimina el último dígito.
    """
    if nit is None:
        return ""

    # Convertir a string y limpiar espacios
    nit_str = str(nit).strip()

    # Validar que sean solo números y cumpla las condiciones
    if nit_str.isdigit() and len(nit_str) == 10 and nit_str[0] in ["8", "9"]:
        return nit_str[:-1]  # quitar el último dígito
    
    return nit_str


# ==========================
#   EJEMPLOS DE USO
# ==========================

ejemplos = [
    "1000351001",   # No cambia (empieza con 1)
    "832001110",    # No cambia (tiene 9 dígitos)
    "8320011101",   # Se le quita el último (832001110)
    "9320011101",   # Se le quita el último (932001110)
    "7320011101",   # No cambia (empieza con 7)
]

print("=== RESULTADOS ===")
for nit in ejemplos:
    print(f"Entrada: {nit}  ->  Salida: {limpiar_nit(nit)}")