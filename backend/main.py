from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import os
from contextlib import asynccontextmanager
import threading
import time
from datetime import datetime, timedelta

from app.routers import sensors, materials, radar, camera, system, control, settings
from app.routers.preservation import router as preservation_router
from app.routers.advanced_features import router as advanced_features_router
from app.routers.api_config import router as api_config_router
from app.routers.ai_analysis import router as ai_analysis_router
from app.routers.artifacts import router as artifacts_router
from app.routers.esp32_data import router as esp32_data_router
from app.websocket import manager, simulate_sensor_stream
from app.services.notifications_service import notifications_service
from app.db import init_db

def start_periodic_data_save():
    """Background task to periodically save sensor data to database every 5 minutes"""
    from app.services.preservation_service import preservation_service
    print("Starting periodic data saving task...")
    
    def periodic_save():
        while True:
            try:
                print(f"[{datetime.now()}] Saving batch of sensor data to database...")
                # In a real implementation, this would save accumulated data
                # For now, just log the activity
                time.sleep(300)  # Sleep for 5 minutes (300 seconds)
            except Exception as e:
                print(f"Error in periodic data save: {e}")
                time.sleep(300)  # Continue with the cycle even if there's an error
    
    # Start the periodic save in a separate thread
    save_thread = threading.Thread(target=periodic_save, daemon=True)
    save_thread.start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database tables
    init_db()
    print("Database initialized")
    
    # Start the periodic data saving task
    start_periodic_data_save()
    
    yield

app = FastAPI(
    title="ArchaeoScan Backend API",
    description="Real-time archaeological monitoring platform backend",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(sensors.router, prefix="/api", tags=["sensors"])
app.include_router(materials.router, prefix="/api", tags=["materials"])
app.include_router(radar.router, prefix="/api", tags=["radar"])
app.include_router(camera.router, prefix="/api", tags=["camera"])
app.include_router(system.router, prefix="/api", tags=["system"])
app.include_router(control.router, prefix="/api", tags=["control"])
app.include_router(settings.router, prefix="/api", tags=["settings"])
app.include_router(preservation_router, prefix="/api", tags=["preservation"])
app.include_router(advanced_features_router, prefix="/api", tags=["advanced"])
app.include_router(api_config_router, prefix="/api", tags=["config"])
app.include_router(ai_analysis_router, prefix="/api/ai", tags=["ai"])
app.include_router(artifacts_router, prefix="/api", tags=["artifacts"])
app.include_router(esp32_data_router, prefix="/api/esp32", tags=["esp32"])

@app.get("/")
async def root():
    return {"message": "Welcome to ArchaeoScan Backend API"}

@app.get("/mini-site")
async def mini_site():
    """Serve mini monitoring site for ESP32/Raspberry Pi"""
    from fastapi.responses import HTMLResponse
    try:
        with open("mini_site.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Mini site not found</h1>", status_code=404)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    # Add websocket to notifications service as well
    notifications_service.add_websocket(websocket)
    
    try:
        # Start the sensor simulation if not already running
        if not manager.is_broadcasting:
            manager.start_broadcasting()
            simulation_task = asyncio.create_task(simulate_sensor_stream())
        
        while True:
            # Keep the connection alive
            data = await websocket.receive_text()
            # Process any incoming commands if needed
            await manager.send_personal_message(f"Echo: {data}", websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        notifications_service.remove_websocket(websocket)
        # If no more connections, stop the simulation
        if len(manager.active_connections) == 0 and manager.is_broadcasting:
            await manager.stop_broadcasting()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)