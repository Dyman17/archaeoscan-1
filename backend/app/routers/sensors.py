from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import base64
import binascii
import asyncio

from app import models, db
from app.schemas import (
    SensorHistoricalData, SensorReadingResponse, SensorType, SensorReadingRequest,
    CalibrationCommand, ControlCommand, ConnectionStatus, SystemStatusResponse,
    SpectrometerReadingRequest, RadarReadingRequest, CameraReadingRequest,
    MaterialClassificationRequest, PreservationIndexRequest, ReportRequest,
    CalibrationResponse, ReportResponse, TimeRangeFilter, AnalysisResult
)
from app.services.preservation_index import calculate_multi_point_preservation
# from app.services.material_classification import classify_material  # Disabled to avoid sklearn dependency
from app.websocket import manager, SensorData
from datetime import datetime

router = APIRouter()

@router.get("/sensors", response_model=SensorHistoricalData)
def get_historical_sensor_data(
    start_time: Optional[datetime] = Query(None, description="Start time for filtering"),
    end_time: Optional[datetime] = Query(None, description="End time for filtering"),
    sensor_types: Optional[List[SensorType]] = Query(None, description="List of sensor types to filter"),
    device_id: Optional[str] = Query(None, description="Device ID to filter"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    db: Session = Depends(db.get_db)
):
    """
    Get historical sensor data with optional filtering by time range, sensor type, and device.
    """
    query = db.query(models.SensorReading)
    
    if start_time:
        query = query.filter(models.SensorReading.timestamp >= start_time)
    if end_time:
        query = query.filter(models.SensorReading.timestamp <= end_time)
    if sensor_types:
        query = query.filter(models.SensorReading.sensor_type.in_(sensor_types))
    if device_id:
        query = query.filter(models.SensorReading.device_id == device_id)
    
    query = query.order_by(models.SensorReading.timestamp.desc())
    query = query.offset(skip).limit(limit)
    
    sensor_readings = query.all()
    
    # Convert SQLAlchemy objects to Pydantic models
    response_readings = []
    for reading in sensor_readings:
        response_readings.append(SensorReadingResponse(
            id=reading.id,
            timestamp=reading.timestamp,
            sensor_type=reading.sensor_type,
            value=reading.value,
            values_json=reading.values_json,
            unit=reading.unit,
            location_lat=reading.location_lat,
            location_lng=reading.location_lng,
            accuracy=reading.accuracy,
            battery_level=reading.battery_level,
            device_id=reading.device_id
        ))
    
    # Count total records for pagination
    total_count_query = db.query(models.SensorReading)
    if start_time:
        total_count_query = total_count_query.filter(models.SensorReading.timestamp >= start_time)
    if end_time:
        total_count_query = total_count_query.filter(models.SensorReading.timestamp <= end_time)
    if sensor_types:
        total_count_query = total_count_query.filter(models.SensorReading.sensor_type.in_(sensor_types))
    if device_id:
        total_count_query = total_count_query.filter(models.SensorReading.device_id == device_id)
    
    total_count = total_count_query.count()
    
    return SensorHistoricalData(
        sensor_readings=response_readings,
        total_count=total_count
    )

@router.post("/sensors/esp32")
def create_esp32_sensor_reading(
    sensor_data: dict,
    db: Session = Depends(db.get_db)
):
    """
    Create sensor readings from ESP32 device.
    Accepts raw JSON data from ESP32 sensors.
    """
    try:
        # Log received data for debugging
        print(f"[ESP32] Received data: {sensor_data}")
        
        # Extract common fields
        device_id = sensor_data.get("esp32_id", "unknown_esp32")
        timestamp = datetime.utcnow()
        
        # Process magnetometer data
        if "mag_x" in sensor_data and "mag_y" in sensor_data and "mag_z" in sensor_data:
            mag_values = {
                "x": sensor_data["mag_x"],
                "y": sensor_data["mag_y"], 
                "z": sensor_data["mag_z"]
            }
            db_mag = models.SensorReading(
                timestamp=timestamp,
                sensor_type="magnetometer",
                values_json=mag_values,
                unit="microtesla",
                device_id=device_id
            )
            db.add(db_mag)
        
        # Process temperature
        if "temperature" in sensor_data:
            db_temp = models.SensorReading(
                timestamp=timestamp,
                sensor_type="temperature",
                value=sensor_data["temperature"],
                unit="celsius",
                device_id=device_id
            )
            db.add(db_temp)
        
        # Process TDS
        if "tds" in sensor_data:
            db_tds = models.SensorReading(
                timestamp=timestamp,
                sensor_type="tds",
                value=sensor_data["tds"],
                unit="ppm",
                device_id=device_id
            )
            db.add(db_tds)
        
        # Process turbidity
        if "turbidity" in sensor_data:
            db_turb = models.SensorReading(
                timestamp=timestamp,
                sensor_type="turbidity",
                value=sensor_data["turbidity"],
                unit="NTU",
                device_id=device_id
            )
            db.add(db_turb)
        
        # Process distance
        if "distance" in sensor_data:
            db_dist = models.SensorReading(
                timestamp=timestamp,
                sensor_type="distance",
                value=sensor_data["distance"],
                unit="cm",
                device_id=device_id
            )
            db.add(db_dist)
        
        # Process piezo
        if "piezo" in sensor_data:
            db_piezo = models.SensorReading(
                timestamp=timestamp,
                sensor_type="vibration",
                value=sensor_data["piezo"],
                unit="raw",
                device_id=device_id
            )
            db.add(db_piezo)
        
        # Process GPS data
        if "latitude" in sensor_data and "longitude" in sensor_data:
            db_gps = models.SensorReading(
                timestamp=timestamp,
                sensor_type="gps",
                values_json={
                    "lat": sensor_data["latitude"],
                    "lng": sensor_data["longitude"],
                    "accuracy": sensor_data.get("accuracy", 0)
                },
                device_id=device_id
            )
            db.add(db_gps)
        
        db.commit()
        
        # Prepare sensor data for WebSocket broadcast
        sensors_payload = {}
        
        # Add available sensor data to payload
        if "temperature" in sensor_data:
            sensors_payload["temperature"] = sensor_data["temperature"]
        if "tds" in sensor_data:
            sensors_payload["tds"] = sensor_data["tds"]
        if "turbidity" in sensor_data:
            sensors_payload["turbidity"] = sensor_data["turbidity"]
        if "distance" in sensor_data:
            sensors_payload["distance"] = sensor_data["distance"]
        if "piezo" in sensor_data:
            sensors_payload["vibration"] = sensor_data["piezo"]
        if "mag_x" in sensor_data and "mag_y" in sensor_data and "mag_z" in sensor_data:
            sensors_payload["magnetometer"] = [sensor_data["mag_x"], sensor_data["mag_y"], sensor_data["mag_z"]]
        if "latitude" in sensor_data and "longitude" in sensor_data:
            sensors_payload["gps"] = {
                "lat": sensor_data["latitude"],
                "lng": sensor_data["longitude"],
                "accuracy": sensor_data.get("accuracy", 0)
            }
        if "battery" in sensor_data:
            sensors_payload["battery"] = sensor_data["battery"]
        
        # Calculate preservation data using the preservation service
        try:
            from routers.services.preservation_service import preservation_service, SensorData as PreservSensorData
            preserv_sensor_data = PreservSensorData(
                temperature=sensor_data.get("temperature", 0),
                turbidity=sensor_data.get("turbidity", 0),
                tds=sensor_data.get("tds", 0),
                pressure=sensor_data.get("pressure", 0),
                humidity=sensor_data.get("humidity", 0),
                distance=sensor_data.get("distance", 0)
            )
            preservation_report = preservation_service.calculate_preservation_report(preserv_sensor_data)
            sensors_payload["preservation"] = preservation_report
            
            # Save sensor data to database
            import asyncio
            asyncio.create_task(save_sensor_data_to_db(preserv_sensor_data, preservation_report, device_id))
            
        except Exception as e:
            print(f"[WARNING] Failed to calculate preservation: {str(e)}")
            # Fallback: send basic preservation data with all materials at 100%
            materials_fallback = {
                "wood": 100, "paper": 100, "fabric": 100, "leather": 100, "bone": 100,
                "lead": 100, "copper": 100, "brass": 100, "tin": 100, "zinc": 100,
                "iron": 100, "steel": 100, "ceramic": 100, "clay": 100, "soft_stone": 100,
                "hard_stone": 100, "glass": 100, "plastic": 100, "rubber": 100, "quartz": 100,
                "gold": 100, "silver": 100, "platinum": 100, "porcelain": 100, "marble": 100,
                "bronze": 100, "asphalt": 100, "ebonite": 100, "fired_clay": 100, "obsidian": 100
            }
            sensors_payload["preservation"] = {
                "water_preservation": 100,
                "materials": materials_fallback,
                "final_preservation": 100
            }
        
        # Create sensor data object for WebSocket
        ws_sensor_data = SensorData(
            timestamp=int(timestamp.timestamp() * 1000),
            sensors=sensors_payload
        )
        
        # Broadcast to all WebSocket clients using the thread-safe method
        print(f"[DEBUG] About to broadcast sensor data via WebSocket: {ws_sensor_data}")
        
        # Use the new thread-safe method to broadcast from synchronous context
        try:
            manager.send_sensor_data_from_thread(ws_sensor_data)
            print("[DEBUG] Successfully scheduled broadcast from thread")
        except Exception as e:
            print(f"[DEBUG] Failed to broadcast via thread-safe method: {e}")
        
        return {
            "message": "ESP32 sensor data received successfully",
            "device_id": device_id,
            "timestamp": timestamp.isoformat(),
            "fields_processed": len([k for k in sensor_data.keys() if k != "esp32_id"]) 
        }
    except Exception as e:
        db.rollback()
        print(f"[ERROR] Failed to process ESP32 data: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to process sensor data: {str(e)}")

@router.get("/sensors/esp32")
def get_latest_esp32_data(
    db: Session = Depends(db.get_db)
):
    """
    Get latest sensor readings from ALL devices.
    """
    # Get latest sensor readings from all devices
    latest_readings = db.query(models.SensorReading)
    latest_readings = latest_readings.order_by(models.SensorReading.timestamp.desc())
    latest_readings = latest_readings.limit(100)  # Get last 100 readings
    
    readings = latest_readings.all()
    
    # Group by device_id and sensor type
    sensor_data = {}
    for reading in readings:
        device_key = reading.device_id
        if device_key not in sensor_data:
            sensor_data[device_key] = {}
        
        sensor_type = reading.sensor_type
        if sensor_type not in sensor_data[device_key]:
            sensor_data[device_key][sensor_type] = []
        
        data_point = {
            "timestamp": reading.timestamp.isoformat(),
            "value": reading.value,
            "unit": reading.unit,
            "values_json": reading.values_json
        }
        sensor_data[device_key][sensor_type].append(data_point)
    
    return {
        "all_devices_data": sensor_data,
        "total_readings": len(readings)
    }

@router.post("/sensors")
def create_sensor_reading(
    sensor_reading: SensorReadingRequest,
    db: Session = Depends(db.get_db)
):
    """
    Create a new sensor reading entry.
    """
    db_sensor_reading = models.SensorReading(
        timestamp=datetime.fromtimestamp(sensor_reading.timestamp / 1000) if sensor_reading.timestamp else datetime.utcnow(),
        sensor_type=sensor_reading.sensor_type,
        value=sensor_reading.value,
        values_json=sensor_reading.values_json,
        unit=sensor_reading.unit,
        location_lat=sensor_reading.location_lat,
        location_lng=sensor_reading.location_lng,
        accuracy=sensor_reading.accuracy,
        battery_level=sensor_reading.battery_level,
        device_id=sensor_reading.device_id
    )
    
    db.add(db_sensor_reading)
    db.commit()
    db.refresh(db_sensor_reading)
    
    return {"message": "Sensor reading created successfully", "id": db_sensor_reading.id}

@router.post("/calibrate")
def calibrate_sensor(calibration_cmd: CalibrationCommand, db: Session = Depends(db.get_db)):
    """
    Send calibration commands to sensors.
    """
    # Log the calibration command
    db_calibration = models.CalibrationData(
        sensor_type=calibration_cmd.sensor_type,
        device_id="system",  # Could be dynamic based on request context
        calibration_parameters=calibration_cmd.parameters or {}
    )
    
    db.add(db_calibration)
    db.commit()
    db.refresh(db_calibration)
    
    # In a real implementation, this would send the command to the physical sensor
    # For now, we'll just return a success message
    return {
        "message": f"Calibration command '{calibration_cmd.command}' sent to {calibration_cmd.sensor_type}",
        "command": calibration_cmd.command,
        "parameters": calibration_cmd.parameters,
        "calibration_id": db_calibration.id
    }

@router.get("/system/status", response_model=SystemStatusResponse)
def get_system_status(device_id: Optional[str] = Query("default_device", description="Device ID to query"), db: Session = Depends(db.get_db)):
    """
    Get current system health status.
    """
    # Get the most recent status for the device
    system_status = db.query(models.SystemStatus)\
        .filter(models.SystemStatus.device_id == device_id)\
        .order_by(models.SystemStatus.timestamp.desc())\
        .first()
    
    if not system_status:
        # Return a default status if none exists
        return SystemStatusResponse(
            id=0,
            timestamp=datetime.utcnow(),
            device_id=device_id,
            battery_level=85.0,
            connection_status=ConnectionStatus.CONNECTED,
            sensor_statuses={
                "temperature": "ok",
                "humidity": "ok", 
                "pressure": "ok",
                "tds": "ok",
                "turbidity": "ok",
                "distance": "ok",
                "magnetometer": "ok",
                "accelerometer": "ok",
                "gyroscope": "ok"
            },
            active_alerts=[],
            cpu_usage=25.0,
            memory_usage=45.0,
            disk_usage=60.0
        )
    
    return SystemStatusResponse(
        id=system_status.id,
        timestamp=system_status.timestamp,
        device_id=system_status.device_id,
        battery_level=system_status.battery_level,
        connection_status=system_status.connection_status,
        sensor_statuses=system_status.sensor_statuses,
        active_alerts=system_status.active_alerts,
        cpu_usage=system_status.cpu_usage,
        memory_usage=system_status.memory_usage,
        disk_usage=system_status.disk_usage
    )

@router.post("/control")
def send_control_command(control_cmd: ControlCommand):
    """
    Send control commands to connected devices or start scans.
    """
    # In a real implementation, this would send the command to the physical device
    # For now, we'll just return a success message
    return {
        "message": f"Control command '{control_cmd.command}' executed successfully",
        "command": control_cmd.command,
        "parameters": control_cmd.parameters,
        "status": "executed"
    }

@router.get("/reports", response_model=List[ReportResponse])
def get_reports(
    report_type: Optional[str] = Query(None, description="Type of report to filter"),
    start_time: Optional[datetime] = Query(None, description="Start time for filtering"),
    end_time: Optional[datetime] = Query(None, description="End time for filtering"),
    device_id: Optional[str] = Query(None, description="Device ID to filter"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    db: Session = Depends(db.get_db)
):
    """
    Get analysis and historical results reports.
    """
    query = db.query(models.Report)
    
    if report_type:
        query = query.filter(models.Report.report_type == report_type)
    if start_time:
        query = query.filter(models.Report.timestamp >= start_time)
    if end_time:
        query = query.filter(models.Report.timestamp <= end_time)
    if device_id:
        query = query.filter(models.Report.device_id == device_id)
    
    query = query.order_by(models.Report.timestamp.desc())
    query = query.offset(skip).limit(limit)
    
    reports = query.all()
    
    response_reports = []
    for report in reports:
        response_reports.append(ReportResponse(
            id=report.id,
            timestamp=report.timestamp,
            report_type=report.report_type,
            content=report.content,
            location_lat=report.location_lat,
            location_lng=report.location_lng,
            device_id=report.device_id,
            generated_by_ai=report.generated_by_ai
        ))
    
    return response_reports

@router.post("/reports/generate")
def generate_analysis_report(
    time_range: TimeRangeFilter,
    db: Session = Depends(db.get_db)
):
    """
    Generate an analysis report based on sensor data in the specified time range.
    """
    # Get sensor readings in the specified time range
    query = db.query(models.SensorReading)
    
    if time_range.start_time:
        query = query.filter(models.SensorReading.timestamp >= time_range.start_time)
    if time_range.end_time:
        query = query.filter(models.SensorReading.timestamp <= time_range.end_time)
    if time_range.sensor_types:
        query = query.filter(models.SensorReading.sensor_type.in_(time_range.sensor_types))
    
    sensor_readings = query.all()
    
    # Convert to dictionary format for preservation calculation
    reading_dicts = []
    for reading in sensor_readings:
        reading_dict = {
            'timestamp': int(reading.timestamp.timestamp() * 1000),
            'sensors': {
                reading.sensor_type: reading.value
            }
        }
        if reading.values_json:
            reading_dict['sensors'][reading.sensor_type] = reading.values_json
        if reading.location_lat and reading.location_lng:
            reading_dict['sensors']['gps'] = {
                'lat': reading.location_lat,
                'lng': reading.location_lng,
                'accuracy': reading.accuracy
            }
        reading_dicts.append(reading_dict)
    
    # Calculate preservation index
    preservation_result = calculate_multi_point_preservation(reading_dicts)
    
    # Create report content
    report_content = {
        'analysis_period': {
            'start': time_range.start_time.isoformat() if time_range.start_time else None,
            'end': time_range.end_time.isoformat() if time_range.end_time else None
        },
        'sensor_data_summary': {
            'total_readings': len(sensor_readings),
            'sensor_types_covered': list(set([sr.sensor_type for sr in sensor_readings]))
        },
        'preservation_analysis': preservation_result,
        'recommendations': []  # Would come from preservation service
    }
    
    # Create report entry
    db_report = models.Report(
        report_type="analysis",
        content=report_content,
        device_id="system",
        generated_by_ai=True
    )
    
    db.add(db_report)
    db.commit()
    db.refresh(db_report)
    
    return ReportResponse(
        id=db_report.id,
        timestamp=db_report.timestamp,
        report_type=db_report.report_type,
        content=db_report.content,
        location_lat=db_report.location_lat,
        location_lng=db_report.location_lng,
        device_id=db_report.device_id,
        generated_by_ai=db_report.generated_by_ai
    )

async def save_sensor_data_to_db(sensor_data_obj, preservation_report, device_id):
    """Save sensor data to database asynchronously"""
    try:
        from sqlalchemy.orm import Session
        from app.db import get_db
        from models.database_models import SensorData as DbSensorData
        import json
        
        # Get database session
        db_gen = get_db()
        db: Session = next(db_gen)
        
        try:
            # Create database record
            db_record = DbSensorData(
                esp32_id=device_id,
                timestamp=datetime.utcnow(),
                turbidity=getattr(sensor_data_obj, 'turbidity', 0),
                temperature=getattr(sensor_data_obj, 'temperature', 0),
                tds=getattr(sensor_data_obj, 'tds', 0),
                pressure=getattr(sensor_data_obj, 'pressure', 0),
                humidity=getattr(sensor_data_obj, 'humidity', 0),
                distance=getattr(sensor_data_obj, 'distance', 0),
                water_preservation=preservation_report.get('water_preservation', 0),
                material_preservation=json.dumps(preservation_report.get('materials', {})),
                final_preservation=preservation_report.get('final_preservation', 0)
            )
            
            db.add(db_record)
            db.commit()
            db.refresh(db_record)
            
            print(f"Saved sensor data to DB: ID {db_record.id}, ESP32 {device_id}")
            
        finally:
            db.close()
            
    except Exception as e:
        print(f"[ERROR] Failed to save sensor data to DB: {str(e)}")
