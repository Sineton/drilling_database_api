"""
Unit conversion utilities
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class UnitConversion:
    """Unit conversion definition"""
    from_unit: str
    to_unit: str
    factor: float
    offset: float = 0.0


class UnitConverter:
    """Utility class for unit conversions"""
    
    # Standard conversions
    CONVERSIONS = {
        # Weight/Force
        ("tonn", "kN"): UnitConversion("tonn", "kN", 9.80665),
        ("t", "kN"): UnitConversion("t", "kN", 9.80665),
        ("klb", "kN"): UnitConversion("klb", "kN", 4.44822),
        ("lb", "N"): UnitConversion("lb", "N", 4.44822),
        
        # Pressure
        ("atm", "bar"): UnitConversion("atm", "bar", 1.01325),
        ("atm", "psi"): UnitConversion("atm", "psi", 14.6959),
        ("psi", "bar"): UnitConversion("psi", "bar", 0.0689476),
        ("kPa", "bar"): UnitConversion("kPa", "bar", 0.01),
        ("MPa", "bar"): UnitConversion("MPa", "bar", 10.0),
        
        # Length
        ("m", "ft"): UnitConversion("m", "ft", 3.28084),
        ("ft", "m"): UnitConversion("ft", "m", 0.3048),
        
        # Flow
        ("l/sek", "l/min"): UnitConversion("l/sek", "l/min", 60.0),
        ("l/s", "l/min"): UnitConversion("l/s", "l/min", 60.0),
        ("gpm", "l/min"): UnitConversion("gpm", "l/min", 3.78541),
        
        # Density
        ("g/sm3", "kg/m3"): UnitConversion("g/sm3", "kg/m3", 1000.0),
        ("g/cm3", "kg/m3"): UnitConversion("g/cm3", "kg/m3", 1000.0),
        ("ppg", "kg/m3"): UnitConversion("ppg", "kg/m3", 119.826),
        
        # Temperature
        ("degC", "degF"): UnitConversion("degC", "degF", 1.8, 32.0),
        ("C", "F"): UnitConversion("C", "F", 1.8, 32.0),
        
        # Torque
        ("kNm", "ft-lb"): UnitConversion("kNm", "ft-lb", 737.562),
        ("kHm", "kNm"): UnitConversion("kHm", "kNm", 1.0),  # Assuming kHm is kilo-Newton-meter
    }
    
    @classmethod
    def convert(cls, value: float, from_unit: str, to_unit: str) -> float:
        """Convert value from one unit to another"""
        if from_unit == to_unit:
            return value
        
        key = (from_unit.lower(), to_unit.lower())
        
        if key in cls.CONVERSIONS:
            conv = cls.CONVERSIONS[key]
            return value * conv.factor + conv.offset
        
        # Try reverse conversion
        reverse_key = (to_unit.lower(), from_unit.lower())
        if reverse_key in cls.CONVERSIONS:
            conv = cls.CONVERSIONS[reverse_key]
            return (value - conv.offset) / conv.factor
        
        # Unknown conversion - return original
        return value
    
    @classmethod
    def get_factor(cls, from_unit: str, to_unit: str) -> float:
        """Get conversion factor"""
        if from_unit == to_unit:
            return 1.0
        
        key = (from_unit.lower(), to_unit.lower())
        
        if key in cls.CONVERSIONS:
            return cls.CONVERSIONS[key].factor
        
        reverse_key = (to_unit.lower(), from_unit.lower())
        if reverse_key in cls.CONVERSIONS:
            return 1.0 / cls.CONVERSIONS[reverse_key].factor
        
        return 1.0
    
    @classmethod
    def apply_conversions(cls, data: Dict[str, Any], conversions: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Apply multiple conversions to data dictionary"""
        result = data.copy()
        
        for field, conv_config in conversions.items():
            if field in result and result[field] is not None:
                from_unit = conv_config.get("from", "")
                to_unit = conv_config.get("to", "")
                factor = conv_config.get("factor")
                
                if factor:
                    result[field] = result[field] * factor
                elif from_unit and to_unit:
                    result[field] = cls.convert(result[field], from_unit, to_unit)
        
        return result
