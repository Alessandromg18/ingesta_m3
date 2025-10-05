import os
import pandas as pd
import boto3
import json
from pymongo import MongoClient

# ================================
# 🔧 CONFIGURACIÓN CON VARIABLES DE ENTORNO
# ================================

MONGODB_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]

# Colecciones a exportar
COLLECTIONS = {
    "UserTiktokMetrics": "user_tiktok_metrics/",
    "AdminTiktokMetrics": "admin_tiktok_metrics/"
}

# S3
BUCKET_NAME = os.getenv("BUCKET_NAME", "my-bucket")

# ================================
# 🧾 SCHEMAS PARA CASTING DE TIPOS
# ================================

SCHEMAS = {
    "UserTiktokMetrics": [
        {"Name": "postId", "Type": "string"},
        {"Name": "datePosted", "Type": "date"},
        {"Name": "hourPosted", "Type": "string"},
        {"Name": "usernameTiktokAccount", "Type": "string"},
        {"Name": "postURL", "Type": "string"},
        {"Name": "views", "Type": "int"},
        {"Name": "likes", "Type": "int"},
        {"Name": "comments", "Type": "int"},
        {"Name": "saves", "Type": "int"},
        {"Name": "reposts", "Type": "int"},
        {"Name": "totalInteractions", "Type": "int"},
        {"Name": "engagement", "Type": "float"},
        {"Name": "numberHashtags", "Type": "int"},
        {"Name": "hashtags", "Type": "string"},
        {"Name": "soundId", "Type": "string"},
        {"Name": "soundURL", "Type": "string"},
        {"Name": "regionPost", "Type": "string"},
        {"Name": "dateTracking", "Type": "date"},
        {"Name": "timeTracking", "Type": "string"},
        {"Name": "userId", "Type": "int"}
    ],
    "AdminTiktokMetrics": [
        {"Name": "postId", "Type": "string"},
        {"Name": "datePosted", "Type": "date"},
        {"Name": "hourPosted", "Type": "string"},
        {"Name": "usernameTiktokAccount", "Type": "string"},
        {"Name": "postURL", "Type": "string"},
        {"Name": "views", "Type": "int"},
        {"Name": "likes", "Type": "int"},
        {"Name": "comments", "Type": "int"},
        {"Name": "saves", "Type": "int"},
        {"Name": "reposts", "Type": "int"},
        {"Name": "totalInteractions", "Type": "int"},
        {"Name": "engagement", "Type": "float"},
        {"Name": "numberHashtags", "Type": "int"},
        {"Name": "hashtags", "Type": "string"},
        {"Name": "soundId", "Type": "string"},
        {"Name": "soundURL", "Type": "string"},
        {"Name": "regionPost", "Type": "string"},
        {"Name": "dateTracking", "Type": "date"},
        {"Name": "timeTracking", "Type": "string"},
        {"Name": "adminId", "Type": "int"}
    ]
}

# ================================
# 🚀 CLIENTES
# ================================

mongo_client = MongoClient(MONGODB_URI)
db = mongo_client[MONGODB_DB]

s3 = boto3.client("s3")

# ================================
# 🧼 FUNCIONES DE LIMPIEZA Y EXPORTACIÓN
# ================================

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia strings para asegurar compatibilidad con Athena"""
    for col in df.select_dtypes(include=["object", "bool"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[\r\n\t]", " ", regex=True)
            .str.replace(r"[^\x20-\x7E]", "", regex=True)
        )
    return df


def cast_types(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """Convierte columnas a los tipos definidos en schema"""
    for col in schema:
        name, typ = col["Name"], col["Type"]
        if name not in df.columns:
            continue
        if typ == "int":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif typ == "float":
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif typ == "date":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.strftime("%Y-%m-%d")
        else:
            df[name] = df[name].astype(str)
    return df


def export_to_ndjson(df: pd.DataFrame, filename: str):
    """Exporta a NDJSON (una línea por JSON)"""
    with open(filename, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")


# ================================
# 🏁 MAIN
# ================================

def main():
    for collection_name, folder in COLLECTIONS.items():
        try:
            print(f"📥 Procesando colección: {collection_name}")
            collection = db[collection_name]
            data = list(collection.find())

            if not data:
                print(f"⚠️ Colección vacía: {collection_name}")
                continue

            df = pd.DataFrame(data)

            if "_id" in df.columns:
                df.drop(columns=["_id"], inplace=True)

            df = clean_dataframe(df)
            df = cast_types(df, SCHEMAS[collection_name])

            filename = f"{collection_name}.json"
            export_to_ndjson(df, filename)

            s3_key = f"{folder}{filename}"

            print(f"⬆️ Subiendo {filename} a s3://{BUCKET_NAME}/{s3_key}")
            s3.upload_file(filename, BUCKET_NAME, s3_key)
            print(f"✅ {collection_name} exportado y subido correctamente.")

            os.remove(filename)

        except Exception as e:
            print(f"❌ Error con {collection_name}: {e}")


if __name__ == "__main__":
    main()
