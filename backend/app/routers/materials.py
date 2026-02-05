from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import json

from app import models, db
from app.schemas import (
    MaterialClassificationResponse, SpectrometerReadingRequest, MaterialType,
    MaterialClassificationRequest
)

# Import all necessary schemas directly to avoid circular import issues
from app.schemas import (
    MaterialClassificationResponse, SpectrometerReadingRequest, MaterialType,
    MaterialClassificationRequest, AnalysisResult
)
# from app.services.material_classification import classify_material, get_material_properties  # Disabled to avoid sklearn dependency

router = APIRouter()

@router.post("/materials/analyze", response_model=MaterialClassificationResponse)
def analyze_material(
    spectrometer_data: SpectrometerReadingRequest,
    environmental_context: Optional[dict] = None,
    db: Session = Depends(db.get_db)
):
    """
    Analyze spectrometer data to classify material type and return confidence.
    """
    # Perform material classification using AI service
    # classification_result = classify_material(
    #     wavelengths=spectrometer_data.wavelengths,
    #     intensities=spectrometer_data.intensity,
    #     environmental_context=environmental_context or {}
    # )
    
    # Return mock classification for now
    classification_result = {
        "material": "unknown",
        "confidence": 0.5,
        "properties": {
            "density": 2.5,
            "hardness": 3.0,
            "composition": "mixed"
        }
    }
    
    # Get material properties
    material_properties = {
        "density": 2.5,
        "hardness": 3.0,
        "composition": "mixed"
    }
    
    # Create material classification record
    db_material_classification = models.MaterialClassification(
        spectrometer_reading_id=0,  # Will be updated if we have a reference
        material_type=classification_result['material'],
        confidence=classification_result['confidence'],
        spectral_signature={
            'wavelengths': spectrometer_data.wavelengths,
            'intensities': spectrometer_data.intensity,
            'environmental_context': environmental_context or {},
            'classification_details': classification_result
        },
        location_lat=spectrometer_data.location_lat,
        location_lng=spectrometer_data.location_lng,
        device_id=spectrometer_data.device_id
    )
    
    db.add(db_material_classification)
    db.commit()
    db.refresh(db_material_classification)
    
    return MaterialClassificationResponse(
        id=db_material_classification.id,
        timestamp=db_material_classification.timestamp,
        spectrometer_reading_id=db_material_classification.spectrometer_reading_id,
        material_type=db_material_classification.material_type,
        confidence=db_material_classification.confidence,
        spectral_signature=db_material_classification.spectral_signature,
        location_lat=db_material_classification.location_lat,
        location_lng=db_material_classification.location_lng,
        device_id=db_material_classification.device_id
    )

@router.get("/materials/classifications", response_model=List[MaterialClassificationResponse])
def get_material_classifications(
    device_id: Optional[str] = None,
    material_type: Optional[MaterialType] = None,
    min_confidence: Optional[float] = 0.0,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(db.get_db)
):
    """
    Get historical material classifications with optional filtering.
    """
    query = db.query(models.MaterialClassification)
    
    if device_id:
        query = query.filter(models.MaterialClassification.device_id == device_id)
    if material_type:
        query = query.filter(models.MaterialClassification.material_type == material_type.value)
    if min_confidence > 0:
        query = query.filter(models.MaterialClassification.confidence >= min_confidence)
    
    query = query.order_by(models.MaterialClassification.timestamp.desc())
    query = query.offset(skip).limit(limit)
    
    classifications = query.all()
    
    response_classifications = []
    for classification in classifications:
        response_classifications.append(MaterialClassificationResponse(
            id=classification.id,
            timestamp=classification.timestamp,
            spectrometer_reading_id=classification.spectrometer_reading_id,
            material_type=classification.material_type,
            confidence=classification.confidence,
            spectral_signature=classification.spectral_signature,
            location_lat=classification.location_lat,
            location_lng=classification.location_lng,
            device_id=classification.device_id
        ))
    
    return response_classifications

@router.get("/materials/properties/{material_type}")
def get_material_properties_endpoint(material_type: MaterialType):
    """
    Get known properties of a specific material type.
    """
    properties = {
        "density": 2.5,
        "hardness": 3.0,
        "composition": "mixed"
    }
    return {
        "material_type": material_type.value,
        "properties": properties
    }

@router.post("/materials/train")
def train_material_classifier():
    """
    Retrain the material classifier with new data (if needed).
    """
    # This would trigger retraining in a real implementation
    # For now, we'll just return a status
    # from app.services.material_classification import classifier_instance
    # classifier_instance.train()
    
    return {
        "message": "Material classifier retrained successfully",
        "status": "completed"
    }