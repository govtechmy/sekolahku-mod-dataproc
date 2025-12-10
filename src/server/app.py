from fastapi import FastAPI

from src.server.api_dataproc import router as dataproc_router

app = FastAPI(
    title="Sekolah Dataproc API",
    version="1.0.0"
)

# Register all dataproc endpoints
app.include_router(dataproc_router)
