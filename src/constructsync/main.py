from fastapi import FastAPI

app = FastAPI(
    title="ConstructSync Pipeline API",
    description="A high-concurrency catalog ingestion, quality assurance, and security sanitization pipeline middleware",
    version="0.1.0"
)

@app.get("/health")
async def health_check():
    return {"status": "ok"}
