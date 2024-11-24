from contextlib import asynccontextmanager
from fastapi import FastAPI
from routers import index_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    print('Server starting up...')
    yield
    print('Server shutting down...')

app = FastAPI(lifespan=lifespan)

app.include_router(index_router.router)

@app.get("/")
async def root():
    return {"message": "Hello World"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
