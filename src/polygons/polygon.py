"""Utility functions for normalizing state and parliament names from polygon data."""
from __future__ import annotations

import re
from typing import Optional

from src.models.negeriEnum import NegeriEnum


def normalize_state_name(raw_state: str) -> Optional[NegeriEnum]:
    """
    Normalize a raw state name from polygon data to NegeriEnum.
    
    Uses intelligent matching against NegeriEnum values without hardcoded mappings.
    Handles various formats like "Johor", "W.P. Kuala Lumpur", "NEGERI_SEMBILAN", etc.
    
    Args:
        raw_state: Raw state name from polygon data (e.g., "Johor", "W.P. Kuala Lumpur")
        
    Returns:
        NegeriEnum value or None if not found
        
    Examples:
        >>> normalize_state_name("Johor")
        <NegeriEnum.JOHOR: 'JOHOR'>
        >>> normalize_state_name("W.P. Kuala Lumpur")
        <NegeriEnum.WILAYAH_PERSEKUTUAN_KUALA_LUMPUR: 'WILAYAH_PERSEKUTUAN_KUALA_LUMPUR'>
        >>> normalize_state_name("Negeri Sembilan")
        <NegeriEnum.NEGERI_SEMBILAN: 'NEGERI_SEMBILAN'>
    """
    if not raw_state:
        return None
    
    # Normalize the input: uppercase, replace spaces/dots with underscores
    normalized_input = raw_state.upper().replace(' ', '_').replace('.', '_')
    
    # Try exact match first
    try:
        return NegeriEnum(normalized_input)
    except ValueError:
        pass
    
    # Handle W.P. (Wilayah Persekutuan) special cases
    if raw_state.startswith('W.P.') or raw_state.upper().startswith('W_P_'):
        # Extract the territory name after W.P.
        territory = re.sub(r'^W[\._]?P[\._]?\s*', '', raw_state, flags=re.IGNORECASE)
        territory_normalized = territory.upper().replace(' ', '_').replace('.', '_')
        
        wp_name = f"WILAYAH_PERSEKUTUAN_{territory_normalized}"
        try:
            return NegeriEnum(wp_name)
        except ValueError:
            pass
    
    # Try matching against all enum values (case-insensitive)
    for negeri in NegeriEnum:
        # Direct match
        if negeri.value == normalized_input:
            return negeri
        
        # Match without underscores (e.g., "NEGERISEMBILAN" matches "NEGERI_SEMBILAN")
        if negeri.value.replace('_', '') == normalized_input.replace('_', ''):
            return negeri
    
    return None


def normalize_parliament_name(raw_parliament: str) -> Optional[str]:
    """
    Normalize a raw parliament name from polygon data.
    
    Converts names like "P.140 Segamat" to "SEGAMAT" (uppercase with underscores).
    Handles special characters and multi-word names.
    
    Args:
        raw_parliament: Raw parliament name from polygon data (e.g., "P.140 Segamat")
        
    Returns:
        Normalized parliament name or None if invalid
        
    Examples:
        >>> normalize_parliament_name("P.140 Segamat")
        'SEGAMAT'
        >>> normalize_parliament_name("P.018 Kulim-Bandar Baharu")
        'KULIM-BANDAR_BAHARU'
        >>> normalize_parliament_name("P.116 Wangsa Maju")
        'WANGSA_MAJU'
    """
    if not raw_parliament:
        return None
    
    # Remove parliament code (e.g., "P.140 ") from the beginning
    # Pattern: P.### followed by space(s)
    cleaned = re.sub(r'^P\.\d+\s+', '', raw_parliament)
    
    if not cleaned:
        return None
    
    # Normalize:
    # 1. Convert to uppercase
    # 2. Replace spaces with underscores
    # 3. Keep hyphens as-is (e.g., "Kulim-Bandar Baharu" -> "KULIM-BANDAR_BAHARU")
    normalized = cleaned.upper().replace(' ', '_')
    
    return normalized


def extract_negeri_from_filename(filename: str) -> Optional[str]:
    """
    Extract state name from extracted polygon filename.
    
    Args:
        filename: Filename like "JOHOR.json" or "W.P._KUALA_LUMPUR.json"
        
    Returns:
        State name string or None
        
    Examples:
        >>> extract_negeri_from_filename("JOHOR.json")
        'JOHOR'
        >>> extract_negeri_from_filename("W.P._KUALA_LUMPUR.json")
        'W.P._KUALA_LUMPUR'
    """
    if not filename:
        return None
    
    # Remove .json extension
    name = filename.replace('.json', '')
    return name


def extract_negeri_parlimen_from_filename(filename: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extract state and parliament names from extracted polygon filename.
    
    Args:
        filename: Filename like "JOHOR_P.140_SEGAMAT.json"
        
    Returns:
        Tuple of (state_name, parliament_name) or (None, None)
        
    Examples:
        >>> extract_negeri_parlimen_from_filename("JOHOR_P.140_SEGAMAT.json")
        ('JOHOR', 'P.140 SEGAMAT')
        >>> extract_negeri_parlimen_from_filename("W.P._KUALA_LUMPUR_P.116_WANGSA_MAJU.json")
        ('W.P._KUALA_LUMPUR', 'P.116 WANGSA_MAJU')
    """
    if not filename:
        return None, None
    
    # Remove .json extension
    name = filename.replace('.json', '')
    
    # Pattern: STATE_P.###_PARLIAMENT_NAME
    # Match everything before _P.\d+_ as state
    match = re.match(r'^(.+?)_(P\.\d+)_(.+)$', name)
    
    if match:
        state_part = match.group(1)
        parliament_code = match.group(2)
        parliament_name = match.group(3).replace('_', ' ')
        
        return state_part, f"{parliament_code} {parliament_name}"
    
    return None, None


def create_negeri_id(negeri: NegeriEnum) -> str:
    """
    Create document ID for negeri collection.
    
    Args:
        negeri: NegeriEnum value
        
    Returns:
        Document ID (same as negeri enum value)
        
    Example:
        >>> create_negeri_id(NegeriEnum.JOHOR)
        'JOHOR'
    """
    return negeri.value


def create_parliament_id(negeri: NegeriEnum, parliament: str) -> str:
    """
    Create document ID for parliament collection.
    
    Args:
        negeri: NegeriEnum value
        parliament: Normalized parliament name
        
    Returns:
        Document ID in format "NEGERI::PARLIAMENT"
        
    Example:
        >>> create_parliament_id(NegeriEnum.PERAK, "TANJONG_MALIM")
        'PERAK::TANJONG_MALIM'
    """
    return f"{negeri.value}::{parliament}"


def validate_geometry(geometry: dict) -> bool:
    """
    Validate that geometry has required fields for GeoJSON.
    
    Args:
        geometry: Geometry dict with 'type' and 'coordinates'
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(geometry, dict):
        return False
    
    if 'type' not in geometry or 'coordinates' not in geometry:
        return False
    
    geom_type = geometry['type']
    if geom_type not in ['Polygon', 'MultiPolygon']:
        return False
    
    coordinates = geometry['coordinates']
    if not isinstance(coordinates, list) or len(coordinates) == 0:
        return False
    
    return True
