from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from xlsxchunk import chunkgenerator
import tempfile
import os
from typing import Optional

app = FastAPI(
    title="Excel Chunk Generator API",
    description="API para generar chunks de texto a partir de archivos Excel",
    version="1.0.0"
)

@app.post("/process-excel/")
async def process_excel(
    file: UploadFile = File(...),
    max_chunks: Optional[int] = Form(100),
    overlap: Optional[int] = Form(10)
):

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        result = chunkgenerator(temp_file_path, max_chunks, overlap)
        
        os.unlink(temp_file_path)
        
        return JSONResponse(
            content=result,
            status_code=200
        )
        
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error procesando el archivo: {str(e)}"},
            status_code=500
        )

@app.get("/")
async def root():
    return {
        "message": "Bienvenido a la API de generación de chunks",
        "usage": "POST /process-excel/ con un archivo Excel para generar chunks"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)