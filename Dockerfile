FROM python:3.10-slim

# Instalación de dependencias
RUN apt-get update && \
    apt-get install -y gcc && \
    pip install --upgrade pip

# Crear directorio de trabajo
WORKDIR /app

# Copiar archivos
COPY . /app

# Instalar dependencias
RUN pip install -r requirements.txt

# Comando de inicio
CMD ["python", "export_to_s3.py"]
